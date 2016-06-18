/*
 * reads.cpp for FR-HIT
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

#include "reads.h"
//#define DEBUG

using namespace std;

void ReadClass::CheckFile(ifstream &fin)
{
    //check fasta or fastq format | for frhit, only fasta format can be processed
    string s1,s2,s3,s4;
    char ch[1000];
    
    fin>>s1; fin.getline(ch, 1000);
    fin>>s2; fin.getline(ch, 1000);
    
    if ('>' == s1[0]) _file_format=1;
    else if ('@' == s1[0])
    {
        fin>>s3;
        fin.getline(ch, 1000);
        fin>>s4;
        fin.getline(ch, 1000);

        _file_format=0;

        if (s2.size() != s4.size()) 
        {
            cerr << "fatal error: fq format, sequence length not equal to quality length\n";
            exit(1);
        }
    }
    else
    {
        cerr << "fatal error: unrecognizable format of reads file.\n";
        exit(1);
    }

    fin.seekg(0);

}

int ReadClass::LoadBatchReads(ifstream &fin)
{
    char ch[1000];
    char c;

    string s;
    
    mreads.resize(BatchNum*param.ncpu);
    vector<ReadInf>::iterator p=mreads.begin();

    for (num=0; num<BatchNum*param.ncpu; num++,p++,_index++) 
    {	
        p->seq.clear();
        p->seq.resize(param.max_read_size);

        string::iterator z = p->seq.begin();
        
        fin>>c;
        if (fin.eof()) break;
        p->index = _index;
        fin>>p->name;
        fin.getline(ch,1000);
        p->length = 0;
        while(!fin.eof()) 
        {
            fin>>c; if (fin.eof()) break;
            fin.unget();
            if (c == '>') break;
            fin>>s;
            if (p->length+s.size() >= param.max_read_size) 
            {
                param.max_read_size+=param.append_read_size;
                p->seq.resize(param.max_read_size);
                z=p->seq.begin() + p->length;
            }

            copy(s.begin(), s.end(), z);
            z += s.size();
            p->length+=s.size();
        }

        p->seq.resize(p->length);
#ifdef DEBUG   // test seq info
        cerr<<" seq "<<p->seq<<endl<<" length "<<p->length<<endl;
#endif
    }
    
    return num;
}

int ReadClass::LoadBatchFastqReads(ifstream &fin)
{
    char ch[1000];
    char c;

    string s;
    string plus;
    string q;
    
    mreads.resize(BatchNum*param.ncpu);
    vector<ReadInf>::iterator p=mreads.begin();

    for (num=0; num<BatchNum*param.ncpu; num++,p++,_index++) 
    {	
        p->seq.clear();
        p->seq.resize(param.max_read_size);

        string::iterator z = p->seq.begin();
        
        //fin>>c;
        if (fin.eof()) break;
        p->index = _index;
        fin>>p->name;
        fin.getline(ch,1000);
        p->length = 0;

        while(!fin.eof())
        {
            fin>>c; if (fin.eof()) break;
            fin.unget();
            if (c == '@') break;
            fin>>s;
            fin>>plus;
            fin>>q;
            if (p->length+s.size() >= param.max_read_size)
            {
                param.max_read_size+=param.append_read_size;
                p->seq.resize(param.max_read_size);
                z=p->seq.begin() + p->length;
            }

            copy(s.begin(), s.end(), z);
            z += s.size();
            p->length+=s.size();
        }

        p->seq.resize(p->length);
#ifdef DEBUG   // test seq info
        cout<<" seq "<<p->seq<<endl<<" length "<<p->length<<endl;
#endif
    }
    
    return num;
}
