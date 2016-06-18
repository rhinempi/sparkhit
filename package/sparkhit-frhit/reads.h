
/*
 * reads.h for FR-HIT
 * Copyright (c) 2010-2011 Beifang Niu All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */


#ifndef _READS_H_
#define _READS_H_

#include<iostream>
#include<cstdlib>
#include<fstream>
#include<string>
#include<vector>

#include "param.h"

using namespace std;

extern Param param;

const int BatchNum=1000;

struct ReadInf
{

    ReadInf()
    {
        seq = "";
        name = "";
        qual = "";
        index = 0;
        length = 0;
    }

    bit32_t index; //serve as id of a read

    string name;   //name of a read
    string seq;    //sequence
    string qual;   //quality score

    int length;    

};

class ReadClass
{

    public:

        ReadClass()
        {
            _index = 0;
        }

        void InitialIndex()
        {
            _index = 0;
        }

        void CheckFile(ifstream &fin);

        int LoadBatchReads(ifstream &fin);
	int LoadBatchFastqReads(ifstream &fin);

public:
        vector< ReadInf > mreads;

        int _file_format;  //0: fq; 1: fa;

        bit32_t num;
        bit32_t _index;
};

#endif //_READS_H_

