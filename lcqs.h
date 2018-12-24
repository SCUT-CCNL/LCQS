/***
    @author: Jiabing Fu, Bixin Ke, Shoubin Dong.
    @date：2018.08.08
    @institute: South China University of Technology
    @Paper: Submitted to Bioinformatics.
***/

#ifndef LCQS_H
#define LCQS_H

#include "libzpaq.h"
#include <vector>
#include <string>
#include <cstdio>
#include <unordered_map>

namespace lcqs {

typedef pthread_t ThreadID; // job ID type

class CompressJob;

struct param {
    int k;
    double threshold;
    const char* out_name;

    param():k(4), threshold(0.1){}

    void set_threshold(double s) {
        if(s < 0.) s = 0.;
        if(s > 1.) s = 1.;
        threshold = s;
    }

    void set_k(int _k) {
        if(_k < 1) _k = 1;
        if(_k > 6) _k = 6;
        k = _k;
    }

    void set_outname(const char* s) {
        out_name = s;
    }
};

struct format {
    char score;
    int32_t qlen[2];

    void write(FILE* f) {
        fwrite(&score, sizeof score, 1, f);
        fwrite(qlen, 4, 2, f);
    }

    void read(FILE* f) {
        fread(&score, sizeof score, 1, f);
        fread(qlen, 4, 2, f);
    }
};

class compressor {
    param par;
    format fmt;
    int threads;
    std::vector<std::string> qs_raw;
    CompressJob* job;
    std::vector<ThreadID> tid;
    ThreadID wid;

    void get_score(const std::vector<std::string>& sample);
    double get_table(const std::vector<std::string>& sample, std::unordered_map<long long, double>& table, int k);
public:
    compressor(int _threads);
    void init(param _par);
    void write_format(FILE* f) { fmt.write(f); }
    void end();
    void qs_add(const std::string& s) { qs_raw.push_back(s); }
    void qs_compress();
};

class ExtractJob;

class decompressor {
    format fmt;
    int threads;
    std::vector<std::string> qs_raw[2];
    ExtractJob* job;
    std::vector<ThreadID> tid;
    FILE* fp;
    void start();
    void end();
    void get_block(uint32_t l, uint32_t r);
public:
    decompressor(int _threads);
    void open(char* s) { fp = fopen(s, "rb"); }
    void read_format();
    void read_content();
    void close() { fclose(fp); }
    void qs_add(libzpaq::StringBuffer& q, int i);
    void get_qs(std::vector<std::string>& ans);
    void query(std::vector<std::string>& ans, uint32_t L, uint32_t R);
    void read_table();
};

}

#endif  // LCQS_H
