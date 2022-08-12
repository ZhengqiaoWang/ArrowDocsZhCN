
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <arrow/filesystem/api.h>

#include <arrow/filesystem/hdfs.h>

#include "arrow_grpc.service.pb.h"

#include <iostream>
#include <string>
#include "common.h"

using namespace std;

// #2: 读取整个文件
std::shared_ptr<arrow::Table> read_whole_file(std::string file_name)
{
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(file_name, arrow::default_memory_pool()));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    std::shared_ptr<arrow::Table> table;
    PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

    return table;
}

int main(int argc, char const *argv[])
{
    auto table =  read_whole_file(PARQUET_FILE_DIR PARQUET_FILE_NAME);
    // auto stream = 
    return 0;
}
