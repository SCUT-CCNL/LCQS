#include "lcqs.h"
#include <stdexcept>
#include <fstream>
#include <vector>
#include <cstdio>
#include <string>
using namespace std;
using namespace libzpaq;

// Handle errors in libzpaq and elsewhere
void libzpaq::error(const char* msg) {
    if (strstr(msg, "ut of memory")) throw std::bad_alloc();
    throw std::runtime_error(msg);
}

void c_test(int argc, char* argv[]) {
    lcqs::compressor co(0);
    lcqs::param par;
    if(argc >= 6) par.set_shreshold(stod(argv[5]));
    if(argc >= 5) par.set_k(stol(argv[4]));
    par.set_outname(argv[3]);
    co.init(par);
    ifstream in(argv[2]);
    string s;
    while(getline(in, s)) co.qs_add(s);
    co.qs_compress();
    co.end();
}

void d_test(int argc, char* argv[]) {
    lcqs::decompressor de(0);
    de.open(argv[2]);
    de.read_format();
    de.read_table();
    de.read_content();
    de.close();
    vector<string> ans;
    de.get_qs(ans);
    ofstream out(argv[3], ios::out);
    for(int i = 0; i < ans.size(); ++i) {
        out << ans[i] << '\n';
    }
}

void r_test(int argc, char* argv[]) {
    lcqs::decompressor de(0);
    de.open(argv[2]);
    de.read_format();
    de.read_table();
    uint32_t l = stoul(argv[4]), r = stoul(argv[5]);
    vector<string> ans;
    de.query(ans, l, r);
    de.close();
    ofstream out(argv[3], ios::out);
    for(int i = 0; i < ans.size(); ++i) {
        out << ans[i] << '\n';
    }
}

int main(int argc, char* argv[])
{
    if(argv[1][0] == 'c') c_test(argc, argv);
    else if(argv[1][0] == 'd') d_test(argc, argv);
    else if(argv[1][0] == 'r') r_test(argc, argv);
    else {
        puts("Invalid option. Please refer to readme for usage.");
        return -1;
    }
    return 0;
}
