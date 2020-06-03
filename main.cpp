/***
    @author: Jiabing Fu, Bixin Ke, Shoubin Dong.
    @date：2019.07.14
    @institute: South China University of Technology
    @Paper: Published in BMC Bioinformatics 2020.
***/

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
    puts("Start Compression...");
    lcqs::compressor co(16);
    lcqs::param par;
    if(argc >= 6) par.set_threshold(stod(argv[5]));
    if(argc >= 5) par.set_k(stol(argv[4]));
    par.set_outname(argv[3]);
    co.init(par);
    par.set_inname(argv[2]);
    co.qs_compress();
    co.end();
    puts("Compression Completed.");
}

void d_test(int argc, char* argv[]) {
    puts("Start Decompression...");
    lcqs::decompressor de(0);
    de.open(argv[2]);
    de.read_format();
    de.read_table();
    de.read_content();
    de.set_out(argv[3]);
    de.get_qs();
    puts("Decompression Completed!");
}

void r_test(int argc, char* argv[]) {
    puts("Start Random decompression...");
    lcqs::decompressor de(0);
    de.open(argv[2]);
    de.read_format();
    de.read_table();
    uint32_t l = stoul(argv[4]), r = stoul(argv[5]);
    de.set_out(argv[3]);
    de.query(l, r);
    puts("Random Decompression Completed!");
}

int main(int argc, char* argv[])
{
    if(argc == 1){
	   puts("Try to type 'lcqs h' for help.");
    }
    else if(argv[1][0] == 'c') c_test(argc, argv);
    else if(argv[1][0] == 'd') d_test(argc, argv);
    else if(argv[1][0] == 'r') r_test(argc, argv);
    else if(argv[1][0] == 'h') {
        puts("\n@author: Jiabing Fu, Bixin Ke, Shoubin Dong.\n@date：2018.08.08.\n@institute: South China University of Technology.\n");
        puts("Usage:\n");
        puts("For compression: lcqs c <input-file> <output-file>. \nExample: lcqs c sample.in sample.lcqs.\n");
        puts("For decompression: lcqs d <input-file> <output-file>. \nExample: lcqs d sample.lcqs sample.in.\n");
        puts("For random decompression: lcqs r <input-file> <output-file> <first-line> <last-line>. \nExample: lcqs r sample.lcqs sample.part 10 100.\n");
    }
    else{ 
        puts("Invalid option. Try to type 'lcqs h' for help.:");
    	return -1;
    }
    return 0;
}
