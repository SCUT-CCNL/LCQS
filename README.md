# LCQS

## Cite

This software implements the LCQS algorithm described in: Fu J , Ke B , Dong S . “LCQS: an efficient lossless compression tool of quality scores with random access functionality” [J]. BMC Bioinformatics, 2020, 21(1).

## Building

LCQS is compiled on 64 bit Linux with g++ and C++11 standard. To build it, run the following command in the main directory:

    make

If building successfully, the executable file _lcqs_ will be generated in the same directory of the source code.

## Usage of LCQS

### Compress

    lcqs c <input-file> <output-file>

In compression mode, LCQS takes a file consists of quality scores of FASTQ format as input.

### Decompress

    lcqs d <input-file> <output-file>

In decompression mode, LCQS takes a compressed LCQS file as input.

### Random access

    lcqs r <input-file> <output-file> <first-line> <last-line>

In random-access mode, LCQS takes a compressed LCQS file as input, and retrieves uncompressed contents as output, from the first line (inclusive) to the last line (inclusive) specified in the parameters.

## Example

An Example is presented with a sample file _sample.in_.

    lcqs c sample.in sample.lcqs

Compress the file _sample.in_ and output to the file _sample.lcqs_.

    lcqs d sample.lcqs sample.in

Decompress the file _sample.lcqs_ and output to the file _sample.in_.

    lcqs r sample.lcqs sample.part 10 100

Fetch the original content from 10th line to 100th line.

# LIBZPAQ

LIBZPAQ is a public domain API of Matt Mahoney's ZPAQ project. The library used in this project is a optimized version based on LIBZPAQ of zpaq 7.15. For detailed descriptions of LIBZPAQ, refer to [ZPAQ](http://mattmahoney.net/dc/zpaq.html).

LIBZPAQ is optimized with vector instruction sets. The context model computations are time-consuming when predicting bits and updating parameters, so we rewrite several parts of them with vector instructions.

To use the optimized version, you need to replace the file _libzpaq.cpp_ with the one of this project. Note that for JIT version, you can compile it with the same instructions. But for NOJIT version, you should replace the option "-msse2" with "-msse4".
