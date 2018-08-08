#include "lcqs.h"
#include "libzpaq.h"
#include <vector>
#include <string>
#include <cstring>
#include <cmath>
#include <algorithm>
#include <pthread.h>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <unordered_map>

using namespace libzpaq;
using namespace std;

namespace lcqs {

typedef void* ThreadReturn;                                // job return type
void run(ThreadID& tid, ThreadReturn(*f)(void*), void* arg)// start job
    {pthread_create(&tid, NULL, f, arg);}
void join(ThreadID tid) {pthread_join(tid, NULL);}         // wait for job
typedef pthread_mutex_t Mutex;                             // mutex type
void init_mutex(Mutex& m) {pthread_mutex_init(&m, 0);}     // init mutex
void lock(Mutex& m) {pthread_mutex_lock(&m);}              // wait for mutex
void release(Mutex& m) {pthread_mutex_unlock(&m);}         // release mutex
void destroy_mutex(Mutex& m) {pthread_mutex_destroy(&m);}  // destroy mutex

class Semaphore {
public:
    Semaphore() {sem=-1;}
    void init(int n) {
        assert(n>=0);
        assert(sem==-1);
        pthread_cond_init(&cv, 0);
        pthread_mutex_init(&mutex, 0);
        sem=n;
    }
    void destroy() {
        assert(sem>=0);
        pthread_mutex_destroy(&mutex);
        pthread_cond_destroy(&cv);
    }
    int wait() {
        assert(sem>=0);
        pthread_mutex_lock(&mutex);
        int r=0;
        if (sem==0) r=pthread_cond_wait(&cv, &mutex);
        assert(sem>0);
        --sem;
        pthread_mutex_unlock(&mutex);
        return r;
    }
    void signal() {
        assert(sem>=0);
        pthread_mutex_lock(&mutex);
        ++sem;
        pthread_cond_signal(&cv);
        pthread_mutex_unlock(&mutex);
    }
private:
    pthread_cond_t cv;  // to signal FINISHED
    pthread_mutex_t mutex; // protects cv
    int sem;  // semaphore count
};

struct BlockInfo {
    enum Tag {QUALITY} tag;
    int64_t pos;
    uint32_t length;
    uint32_t start;
    uint32_t end;
    uint32_t eline;
    uint8_t bucket;

    void write(FILE* f) {
        char _tag = tag;
        fwrite(&_tag, sizeof _tag, 1, f);
        fwrite(&pos, sizeof pos, 1, f);
        fwrite(&length, sizeof length, 1, f);
        fwrite(&start, sizeof start, 1, f);
        fwrite(&end, sizeof end, 1, f);
        fwrite(&eline, sizeof eline, 1, f);
        fwrite(&bucket, sizeof bucket, 1, f);
    }

    bool read(FILE* f) {
        char _tag;
        if(fread(&_tag, sizeof _tag, 1, f) == 0) return false;
        tag = Tag(_tag);
        fread(&pos, sizeof pos, 1, f);
        fread(&length, sizeof length, 1, f);
        fread(&start, sizeof start, 1, f);
        fread(&end, sizeof end, 1, f);
        fread(&eline, sizeof eline, 1, f);
        fread(&bucket, sizeof bucket, 1, f);
        return true;
    }
};

// A CompressJob is a queue of blocks to compress and write to the archive.
// Each block cycles through states EMPTY, FILLING, FULL, COMPRESSING,
// COMPRESSED, WRITING. The main thread waits for EMPTY buffers and
// fills them. A set of compressThreads waits for FULL threads and compresses
// them. A writeThread waits for COMPRESSED buffers at the front
// of the queue and writes and removes them.

// Buffer queue element
struct CJ {
    BlockInfo info;
    enum {EMPTY, FULL, COMPRESSING, COMPRESSED, WRITING} state;
    StringBuffer in;       // uncompressed input
    StringBuffer out;      // compressed output
    string comment;        // if "" use default
    string method;         // compression level or "" to mark end of data
    Semaphore full;        // 1 if in is FULL of data ready to compress
    Semaphore compressed;  // 1 if out contains COMPRESSED data
    CJ(): state(EMPTY) {}
};

struct FileWriter: public libzpaq::Writer {
    FILE* fp;
    FileWriter(const char* filename) {
        fp = fopen(filename, "wb");
    }

    ~FileWriter() {
        if(fp) {
            fclose(fp);
            fp = NULL;
        }
    }

    void seek(int offset) {
        fseek(fp, offset, SEEK_SET);
    }

    void put(int c) {
        putc(c, fp);
    }

    void write(const char* buf, int n) {
        fwrite(buf, 1, n, fp);
    }

    int64_t tell() {
        return ftello(fp);
    }
};

// Instructions to a compression job
class CompressJob {
public:
    Mutex mutex;           // protects state changes
    FileWriter* out;
    char score;
private:
    int job;               // number of jobs
    CJ* q;                 // buffer queue
    unsigned qsize;        // number of elements in q
    int front;             // next to remove from queue
    Semaphore empty;       // number of empty buffers ready to fill
    Semaphore compressors; // number of compressors available to run
public:
    friend ThreadReturn compressThread(void* arg);
    friend ThreadReturn writeThread(void* arg);
    CompressJob(int threads, int buffers): job(0), q(0), qsize(buffers), front(0) {
        q=new CJ[buffers];
        if (!q) throw std::bad_alloc();
        init_mutex(mutex);
        empty.init(buffers);
        compressors.init(threads);
        for (int i=0; i<buffers; ++i) {
            q[i].full.init(0);
            q[i].compressed.init(0);
        }
    }
    ~CompressJob() {
        for (int i=qsize-1; i>=0; --i) {
            q[i].compressed.destroy();
            q[i].full.destroy();
        }
        compressors.destroy();
        empty.destroy();
        destroy_mutex(mutex);
        delete[] q;
    }      
    void write(StringBuffer& s, BlockInfo::Tag _tag, uint8_t bucket, uint32_t start, uint32_t end, uint32_t eline, string method);
    vector<BlockInfo> binfo;
    void write_binfo() {
        for(auto& b : binfo) {
            b.write(out->fp);
        }
    }
};

// Write s at the back of the queue. Signal end of input with method=""
void CompressJob::write(StringBuffer& s, BlockInfo::Tag _tag, uint8_t bucket, uint32_t start, uint32_t end, uint32_t eline, string method) {
    for (unsigned k=(method=="")?qsize:1; k>0; --k) {
        empty.wait();
        lock(mutex);
        unsigned i, j;
        for (i=0; i<qsize; ++i) {
            if (q[j=(i+front)%qsize].state==CJ::EMPTY) {
                q[j].info.tag=_tag;
                q[j].info.bucket=bucket;
                q[j].info.start=start;
                q[j].info.end=end;
                q[j].info.eline=eline;
                q[j].comment="";
                q[j].method=method;
                q[j].in.resize(0);
                q[j].in.swap(s);
                q[j].state=CJ::FULL;
                q[j].full.signal();
                break;
            }
        }
        release(mutex);
        assert(i<qsize);  // queue should not be full
    }
}

void pack(StringBuffer& in, char score) {
    StringBuffer out;
    out.resize(in.size() / 2);
    out.resize(0);
    int len = 0; // pending output bytes
    int j = 0, k = 0, l2 = max(33, score-7), l3 = l2 + 4, r = l2 + 7; // last 2 bytes
	for (int c = 0; (c = in.get()) != EOF; k = j, j = c) {
		if (len == 0 && (c == score || c >= l2 && c <= r)) ++len;
		else if (len == 1 && (c == score && j == score || c >= l3 && c <= r && j >= l3 && j <= r)) ++len;
		else if (len >= 2 && len<55 && k == score && j == score && c == score) ++len;
        else {  // must write pending output
            ++len;  // c is pending
            if (len>2 && j == score && k == score || len==2 && j == score)
                out.put(199 + len), len = 1; // [201, 255]
            if (len == 3) {
                if (c >= l3 && c <= r)
                    out.put(137 + (k - l3) + 4 * (j - l3) + 16 * (c - l3)), len = 0; // [137, 200]
                else
                    out.put(73 + (k - l2) + 8 * (j - l2)), len = 1; // [109, 112], [117, 120], [125, 128], [133, 136]
            }
            if (len == 2) {
                if (c >= l2 && c <= r) out.put(73 + (j - l2) + 8 * (c - l2)), len = 0; // [73, 108], [113, 116], [121, 124], [129, 132]
                else out.put(j - 32), len = 1; // [32, 39]
            }
            if (len == 1) {
                if (c == 10) {
                    len = j = k = 0;
                    out.put(0);
                    continue;
                }
                if (c<l2 || c>r) out.put(c - 32), len = 0; // [4, 31], [40, 72]
            }
        }
    }
    out.swap(in);
}

// Compress data in the background, one per buffer
ThreadReturn compressThread(void* arg) {
    CompressJob& job=*(CompressJob*)arg;
    int jobNumber=0;
    try {

        // Get job number = assigned position in queue
        lock(job.mutex);
        jobNumber=job.job++;
        assert(jobNumber>=0 && jobNumber<int(job.qsize));
        CJ& cj=job.q[jobNumber];
        release(job.mutex);

        // Work until done
        while (true) {
            cj.full.wait();
            lock(job.mutex);

            // Check for end of input
            if (cj.method=="") {
                cj.compressed.signal();
                release(job.mutex);
                return 0;
            }

            // Compress
            assert(cj.state==CJ::FULL);
            cj.state=CJ::COMPRESSING;
            release(job.mutex);
            job.compressors.wait();
            if(cj.info.tag == BlockInfo::QUALITY) pack(cj.in, job.score);
            libzpaq::compressBlock(&cj.in, &cj.out, cj.method.c_str(), "", cj.comment.c_str(), false);
            cj.in.resize(0);
            lock(job.mutex);
            cj.state=CJ::COMPRESSED;
            cj.compressed.signal();
            job.compressors.signal();
            release(job.mutex);
        }
    }
    catch (std::exception& e) {
        lock(job.mutex);
        fflush(stdout);
        fprintf(stderr, "job %d: %s\n", jobNumber+1, e.what());
        release(job.mutex);
        exit(1);
    }
    return 0;
}

void compressor::init(param _par) {
    par = _par;
    job->out = new FileWriter(par.out_name);
    job->out->seek(17);
}

// Write compressed data in the background
ThreadReturn writeThread(void* arg) {
    CompressJob& job=*(CompressJob*)arg;
    try {

        // work until done
        while (true) {

            // wait for something to write
            CJ& cj=job.q[job.front];  // no other threads move front
            cj.compressed.wait();

            // Quit if end of input
            lock(job.mutex);
            if (cj.method=="") {
                release(job.mutex);
                return 0;
            }

            // Write
            assert(cj.state==CJ::COMPRESSED);
            cj.state=CJ::WRITING;
            if (job.out && cj.out.size()>0) {
                release(job.mutex);
                assert(cj.out.c_str());
                const char* p=cj.out.c_str();
                uint32_t n=cj.out.size();
                const uint32_t N=1<<30;
                cj.info.pos=job.out->tell();
                while (n>N) {
                    job.out->write(p, N);
                    p+=N;
                    n-=N;
                }
                job.out->write(p, n);
                cj.info.length=job.out->tell() - cj.info.pos;
                lock(job.mutex);
            }
            cj.out.resize(0);
            cj.state=CJ::EMPTY;
            job.front=(job.front+1)%job.qsize;
            job.binfo.push_back(cj.info);
            job.empty.signal();
            release(job.mutex);
        }
    }
    catch (std::exception& e) {
        fflush(stdout);
        fprintf(stderr, "zpaq exiting from writeThread: %s\n", e.what());
        exit(1);
    }
    return 0;
}

const size_t BUFFER_SIZE = 1 << 25;
const char METHOD[] = "55,220,0";

compressor::compressor(int _threads): threads(_threads) {
    if(threads < 1) threads = thread::hardware_concurrency();
    tid.resize(threads*2-1);
    job = new CompressJob(threads, tid.size());
    for (unsigned i=0; i<tid.size(); ++i) run(tid[i], compressThread, job);
    run(wid, writeThread, job);
}

void compressor::end() {
    StringBuffer _;
    job->write(_, BlockInfo::Tag(0), 0, 0, 0, 0, "");  // signal end of input
    for (unsigned i=0; i<tid.size(); ++i) join(tid[i]);
    join(wid);
    int64_t len = job->out->tell();
    job->write_binfo();
    job->out->seek(0);
    fmt.write(job->out->fp);
    fwrite(&len, sizeof len, 1, job->out->fp);
    delete job->out;
    delete job;
}

void compressor::get_score(const vector<string>& sample) {
    int score_cnt[128];
    memset(score_cnt, 0, sizeof score_cnt);
    for(auto& s : sample) {
        for(char c : s) ++score_cnt[c];
    }
    fmt.score = max_element(score_cnt, score_cnt + 128) - score_cnt;
}

double compressor::get_table(const vector<string>& sample, unordered_map<long long, double>& table, int k) {
    unordered_map<long long, int> mp;
    int tot = 0;
    for(auto& s : sample) {
        for(size_t i = k-1; i < s.size(); ++i) {
            long long val = 0;
            for(size_t l = i+1-k; l <= i; ++l) val = val << 7 | s[l];
            ++mp[val];
		}
		tot += s.size()+1-k;
    }
    vector<pair<int, long long>> vec;
    for(auto& _ : mp) vec.emplace_back(_.second, _.first);
    sort(vec.begin(), vec.end(), greater<pair<int, long long>>());
    int cnt = tot * 0.7;
    for(auto it = vec.begin(); cnt > 0; ++it) {
        table[it->second] = it->first / (double)tot;
        cnt -= it->first;
    }
    double mx = 0;
    for(auto& s : sample) {
        double score = 0;
        for(size_t i = k-1; i < s.size(); ++i) {
            long long val = 0;
            for(size_t l = i+1-k; l <= i; ++l) val = val << 7 | s[l];
            score += table[val];
        }
        mx = max(mx, score/(s.size()-3));
    }
    return mx;
}

void compressor::qs_compress() {
    double border;
    const double shreshold = par.shreshold;
    const int k = par.k;
    unordered_map<long long, double> table;
    table.max_load_factor(0.5);
    {
        vector<string> sample(qs_raw.begin(), qs_raw.size() < 100000 ? qs_raw.end() : qs_raw.begin()+100000);
        get_score(sample);
        border = get_table(sample, table, k) * par.shreshold;
        job->score = fmt.score;
    }
    StringBuffer sb[2];
    uint32_t cur[3]{}, pre[2]{}, eline[2]{};
    long long base = 1;
    for(int i = 1; i < k; ++i) base = base << 7;
    base -= 1;
    for(string& s : qs_raw) {
        long long val = 0;
        for(int i = 0; i < k-1; ++i) val = val << 7 | s[i];
        double score = 0;
        for(size_t j = k-1; j < s.size(); ++j) {
            val = (val & base) << 7 | s[j];
            auto it = table.find(val);
            if(it != table.end()) score += it->second;
        }
        size_t res = score < border*(s.size()+1-k);
        StringBuffer& temp = sb[res];
        ++cur[res];
        temp.write(s.c_str(), s.size());
        temp.put('\n');
        if(res > 0) {
            sb[0].put('\n');
            ++cur[0];
        }
        if(temp.size() > BUFFER_SIZE) {
            job->write(temp, BlockInfo::QUALITY, res, pre[res], cur[res], eline[res], METHOD);
            pre[res] = cur[res];
            eline[res] = cur[res+1];
        }
    }
    for(size_t i = 0; i < 2; ++i) {
        if(sb[i].size() > 0) {
            job->write(sb[i], BlockInfo::QUALITY, i, pre[i], cur[i], eline[i], METHOD);
        }
    }
    for(int i = 0; i < 2; ++i) fmt.qlen[i] = cur[i];
    vector<string>().swap(qs_raw);
}

struct Block {
    StringBuffer* in;
    enum {READY, WORKING, GOOD, BAD} state;
    int id, info;
    Block(int _id = -1, int _info = 0): state(READY), id(_id), info(_info), in(new StringBuffer) {}

    void operator = (const Block& b) {
        in = b.in; state = b.state; id = b.id;
    }
};

struct ExtractJob {         // list of jobs
    Mutex mutex;              // protects state
    int job;                  // number of jobs started
    vector<Block> block;      // list of data blocks to extract
    vector<BlockInfo> binfo;
    vector<string>* qout;
    size_t q_len;
    ExtractJob(): job(0) {
        init_mutex(mutex);
    }
    ~ExtractJob() {
        destroy_mutex(mutex);
    }
};

void unpack(StringBuffer& in, char score) {
    StringBuffer out;
    out.resize(BUFFER_SIZE + 1234);
    out.resize(0);
    int l2 = max(score-7, 33), l3 = l2 + 4;
    for (int i = 0, c = 0; (c = in.get()) != EOF;) {
        static int _ = 0;
		if (c == 0) { // end of line
            out.put(10);
            i = 0;
			continue;
        }
		else if (c >= 201)
			while (c-->200) ++i, out.put(score);
		else if (c >= 137 && c <= 200) {
			c -= 137;
			out.put((c & 3) + l3);
			out.put(((c >> 2) & 3) + l3);
            out.put(((c >> 4) & 3) + l3);
            i += 3;
		}
		else if (c >= 73 && c <= 136) {
			c -= 73;
			out.put((c & 7) + l2);
            out.put(((c >> 3) & 7) + l2);
            i += 2;
		}
		else if (c >= 1 && c <= 72) {
            out.put(c + 32);
            ++i;
		}
    }
    out.swap(in);
}

char score;

// Decompress blocks in a job until none are READY
ThreadReturn decompressThread(void* arg) {
    ExtractJob& job=*(ExtractJob*)arg;
    
    int jobNumber=0;

    // Get job number
    lock(job.mutex);
    jobNumber=++job.job;
    release(job.mutex);

    // Look for next READY job.
    int next=0;  // current job
    while (true) {
        lock(job.mutex);
        for (unsigned i=0; i<=job.block.size(); ++i) {
            unsigned k=i+next;
            if (k>=job.block.size()) k-=job.block.size();
            if (i==job.block.size()) {  // no more jobs?
                release(job.mutex);
                return 0;
            }
            Block& b=job.block[k];
            if (b.state==Block::READY) {
                b.state=Block::WORKING;
                release(job.mutex);
                next=k;
                break;
            }
        }
        Block& b=job.block[next];

        // Decompress
        StringBuffer out, sb;
        Decompresser d;
        d.setInput(b.in);
        d.setOutput(&out);
        d.findBlock();
        d.findFilename();
        d.readComment();
        d.decompress();
        d.readSegmentEnd();
        delete b.in;
        b.in = NULL;

        BlockInfo& info = job.binfo[b.info];

        // Write
        if(b.id == -1) {
            vector<string>& q = job.qout[info.bucket];
            size_t os = info.start;
            unpack(out, score);
            char c;
            while((c=out.get()) != EOF) {
                if(c != '\n') {
                    q[os].push_back(c);
                    while((c=out.get()) != '\n' && c != EOF) q[os].push_back(c);
                }
                ++os;
            }
            continue;
        }
        
    } // end while true

    // Last block
    return 0;
}

decompressor::decompressor(int _threads): threads(_threads), job(new ExtractJob) {
    if(threads < 1) threads = thread::hardware_concurrency();
    job->qout = qs_raw;
}

void decompressor::read_format() {
    fmt.read(fp);
    for(int i = 0; i < 2; ++i) qs_raw[i].resize(fmt.qlen[i]);
}

void decompressor::read_content() {
    for(size_t j = 0; j < job->binfo.size(); ++j) {
        BlockInfo& b = job->binfo[j];
        fseeko(fp, b.pos, SEEK_SET);
        StringBuffer sb;
        sb.resize(b.length);
        fread(sb.data(), 1, b.length, fp);
        qs_add(sb, j);
    }
}

void decompressor::read_table() {
    int64_t pos;
    fread(&pos, sizeof pos, 1, fp);
    fseeko(fp, pos, SEEK_SET);
    BlockInfo info;
    while(info.read(fp)) {
        job->binfo.push_back(info);
    }
}

void decompressor::qs_add(StringBuffer& q, int i) {
    job->block.push_back(Block(-1, i));
    job->block[job->block.size()-1].in->swap(q);
}

void decompressor::start() {
    score = fmt.score;
    tid.resize(threads);
    for (unsigned i=0; i<tid.size(); ++i) run(tid[i], decompressThread, job);
}

void decompressor::end() {
    for (unsigned i=0; i<tid.size(); ++i) join(tid[i]);
    delete job;
}

void decompressor::get_qs(vector<string>& ans) {
    start();
    end();
    size_t a, b;
    for(a = 0, b = 0; a < qs_raw[0].size(); ++a) {
        if(qs_raw[0][a].size() == 0) qs_raw[0][a].swap(qs_raw[1][b++]);
    }
    ans.swap(qs_raw[0]);
}

void decompressor::get_block(uint32_t L, uint32_t R) {
    for(uint8_t i = 0; i < 2; ++i) {
        uint32_t l = 0xffffffffu, r = 0;
        for(size_t j = 0; j < job->binfo.size(); ++j) {
            BlockInfo& b = job->binfo[j];
            if(i == b.bucket && b.end > L && b.start < R) {
                fseeko(fp, b.pos, SEEK_SET);
                StringBuffer sb;
                sb.resize(b.length);
                fread(sb.data(), 1, b.length, fp);
                qs_add(sb, j);
                l = min(l, b.eline);
                r = max(r, b.eline);
            }
        }
        L = l;
        l = r; r = fmt.qlen[(i+1)%2];
        for(auto& b : job->binfo) if(i == b.bucket) {
            if(b.eline > l && b.eline < r) r = b.eline;
        }
        R = r;
    }
}

void decompressor::query(vector<string>& ans, uint32_t _L, uint32_t _R) {
    uint32_t L = _L - 1, R = _R;
    get_block(L, R);
    start();
    end();
    size_t a = L, c;
    for(auto& b : job->binfo) if(b.bucket == 0 && b.start <= L && b.end > L) {
        c = b.eline;
        for(uint32_t i = b.start; i < L; ++i) {
            if(qs_raw[0][i] == "") ++c;
        }
    }
    ans.resize(R-L);
    size_t d;
    while(a < R) {
        if(qs_raw[0][a] == "") ans[d++].swap(qs_raw[1][c++]);
        else ans[d++].swap(qs_raw[0][a]);
        ++a;
    }
}

}
