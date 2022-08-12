
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <arrow/io/hdfs.h>

#include <iostream>
#include <string>
using namespace std;

#define PARQUET_FILE_NAME "test2.parquet"

std::shared_ptr<arrow::Table> generate_table()
{
    arrow::Int64Builder i64builder;
    for (int i = 1; i <= 10; ++i)
    {
        PARQUET_THROW_NOT_OK(i64builder.Append(i));
    }
    std::shared_ptr<arrow::Array> i64array;
    PARQUET_THROW_NOT_OK(i64builder.Finish(&i64array));

    arrow::StringBuilder strbuilder;
    for (int i = 0; i < 2; ++i)
    {
        PARQUET_THROW_NOT_OK(strbuilder.Append("一些"));
        PARQUET_THROW_NOT_OK(strbuilder.Append("字符串"));
        PARQUET_THROW_NOT_OK(strbuilder.Append("文本"));
        PARQUET_THROW_NOT_OK(strbuilder.Append("在"));
        PARQUET_THROW_NOT_OK(strbuilder.Append("这里~"));
    }
    std::shared_ptr<arrow::Array> strarray;
    PARQUET_THROW_NOT_OK(strbuilder.Finish(&strarray));

    std::shared_ptr<arrow::Schema> schema = arrow::schema(
        {arrow::field("int", arrow::int64()), arrow::field("str", arrow::utf8())});

    return arrow::Table::Make(schema, {i64array, strarray});
}

void write_parquet_file(const arrow::Table &table)
{
    std::shared_ptr<arrow::io::FileOutputStream> outfile;

    PARQUET_ASSIGN_OR_THROW(
        outfile, arrow::io::FileOutputStream ::Open(PARQUET_FILE_NAME, false));
    // 该函数调用的最后一个参数是parquet文件中RowGroup的大小。
    // 通常情况下，你会选择相当大的尺寸，但在本例中，我们使用一个小的值来拥有多个RowGroups。
    PARQUET_THROW_NOT_OK(
        parquet::arrow::WriteTable(table, arrow::default_memory_pool(), outfile, 3));
}

// #2: 读取整个文件
void read_whole_file()
{
    std::cout << std::endl
              << "一次性读取 " << PARQUET_FILE_NAME << std::endl;
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(PARQUET_FILE_NAME, arrow::default_memory_pool()));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    std::shared_ptr<arrow::Table> table;
    PARQUET_THROW_NOT_OK(reader->ReadTable(&table));
    std::cout << "=== " << __func__ << " ===" << std::endl;
    std::cout << table->ToString() << std::endl;
    std::cout << "已加载 " << table->num_rows() << " 行，" << table->num_columns() << " 列." << std::endl;
}

// #3: 从文件里只读一个RowGroup
void read_single_rowgroup()
{
    std::cout << std::endl
              << "只读取 " << PARQUET_FILE_NAME << " 中的第一个RowGroup" << std::endl;
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(PARQUET_FILE_NAME, arrow::default_memory_pool()));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    std::shared_ptr<arrow::Table> table;
    PARQUET_THROW_NOT_OK(reader->RowGroup(0)->ReadTable(&table));
    // PARQUET_THROW_NOT_OK(reader->ReadRowGroups({0, 1}, &table));
    std::cout << "=== " << __func__ << " ===" << std::endl;
    std::cout << table->ToString() << std::endl;
    std::cout << "已加载 " << table->num_rows() << " 行，" << table->num_columns() << " 列." << std::endl;
}

// #4: 只读一列
void read_single_column()
{
    std::cout << std::endl
              << "只读取 " << PARQUET_FILE_NAME << " 中的第一列" << std::endl;
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(PARQUET_FILE_NAME, arrow::default_memory_pool()));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    std::shared_ptr<arrow::ChunkedArray> array;
    PARQUET_THROW_NOT_OK(reader->ReadColumn(0, &array));

    std::cout << "=== " << __func__ << " ===" << std::endl;
    std::cout << array->ToString() << std::endl;
    std::cout << "已加载 " << array->length() << " 行." << std::endl;
}

// #5: 只读第一个RowGroup的第一列
void read_single_column_chunk()
{
    std::cout << std::endl
              << "只读取 " << PARQUET_FILE_NAME << " 中的第一个RowGroup的第一列" << std::endl;
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(PARQUET_FILE_NAME, arrow::default_memory_pool()));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    std::shared_ptr<arrow::ChunkedArray> array;
    PARQUET_THROW_NOT_OK(reader->RowGroup(0)->Column(0)->Read(&array));
    std::cout << "=== " << __func__ << " ===" << std::endl;
    std::cout << array->ToString() << std::endl;
    std::cout << "已加载 " << array->length() << " 行." << std::endl;
}

int main(int argc, char const *argv[])
{
    std::shared_ptr<arrow::Table> table = generate_table();
    write_parquet_file(*table);
    read_whole_file();
    read_single_rowgroup();
    read_single_column();
    read_single_column_chunk();
    return 0;
}
