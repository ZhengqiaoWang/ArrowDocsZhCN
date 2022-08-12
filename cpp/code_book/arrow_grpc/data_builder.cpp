
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/filesystem/api.h>

#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

#include <iostream>
#include <vector>
#include <set>
#include <random>
#include <chrono>

#include "common.h"
using namespace std;

class RandomBatchGenerator
{

public:
    std::shared_ptr<arrow::Schema> schema;
    RandomBatchGenerator(std::shared_ptr<arrow::Schema> schema) : schema(schema){};
    arrow::Result<std::shared_ptr<arrow::Table>> Generate(int32_t num_rows)
    {
        num_rows_ = num_rows;
        for (std::shared_ptr<arrow::Field> field : schema->fields())
        {
            ARROW_RETURN_NOT_OK(arrow::VisitTypeInline(*field->type(), this));
        }
        return arrow::Table::Make(schema, arrays_, num_rows);
    }

    // Default implementation
    arrow::Status Visit(const arrow::DataType &type)
    {
        cout << "visit invalid type:" << type.ToString() << endl;
        return arrow::Status::NotImplemented("Generating data for", type.ToString());
    }

    arrow::Status Visit(const arrow::DoubleType &)
    {
        auto builder = arrow::DoubleBuilder();
        std::normal_distribution<> d{/*mean=*/5.0, /*stddev=*/2.0}; // 正态分布
        for (int32_t i = 0; i < num_rows_; ++i)
        {
            builder.Append(d(gen_));
        }

        ARROW_ASSIGN_OR_RAISE(auto array, builder.Finish());
        arrays_.push_back(array);
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::Date32Type &)
    {
        auto builder = arrow::Date32Builder();
        for (int32_t i = 0; i < num_rows_; ++i)
        {
            builder.Append(20210202);
        }
        ARROW_ASSIGN_OR_RAISE(auto array, builder.Finish());
        arrays_.push_back(array);
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::Int64Type &)
    {
        auto builder = arrow::Int64Builder();
        std::normal_distribution<> d{/*mean=*/5.0, /*stddev=*/2.0}; // 正态分布
        for (int32_t i = 0; i < num_rows_; ++i)
        {
            builder.Append(static_cast<int64_t>(d(gen_) * 10000.0));
        }
        ARROW_ASSIGN_OR_RAISE(auto array, builder.Finish());
        arrays_.push_back(array);
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::StringType &)
    {
        auto builder = arrow::StringBuilder();
        for (int32_t i = 0; i < num_rows_; ++i)
        {
            builder.Append(std::string("string:") + to_string(i));
        }
        ARROW_ASSIGN_OR_RAISE(auto array, builder.Finish());
        arrays_.push_back(array);
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::BooleanType &)
    {
        auto builder = arrow::BooleanBuilder();
        for (int32_t i = 0; i < num_rows_; ++i)
        {
            builder.Append(i % 2 == 0);
        }
        ARROW_ASSIGN_OR_RAISE(auto array, builder.Finish());
        arrays_.push_back(array);
        return arrow::Status::OK();
    }

protected:
    std::random_device rd_{};
    std::mt19937 gen_{rd_()}; // 随机种子
    std::vector<std::shared_ptr<arrow::Array>> arrays_;
    int32_t num_rows_;

}; // RandomBatchGenerator

void write_parquet_file(const arrow::Table &table)
{
    std::shared_ptr<arrow::io::FileOutputStream> outfile;

    PARQUET_ASSIGN_OR_THROW(
        outfile, arrow::io::FileOutputStream ::Open(PARQUET_FILE_DIR PARQUET_FILE_NAME, false));
    PARQUET_THROW_NOT_OK(
        parquet::arrow::WriteTable(table, arrow::default_memory_pool(), outfile, PARQUET_ROWGROUP_RECORDS));
}

std::string formatTime(std::chrono::steady_clock::time_point &clock)
{
    auto now_time = std::chrono::steady_clock::now();
    std::string output{""};

    output = std::to_string(std::chrono::duration<double, std::milli>(now_time - clock).count()) + " ms";
    return output;
}

arrow::Status func()
{
    auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
    ARROW_RETURN_NOT_OK(fs->CreateDir(PARQUET_FILE_DIR));
    ARROW_RETURN_NOT_OK(fs->DeleteDirContents(PARQUET_FILE_DIR));

    std::shared_ptr<arrow::Schema> schema = getSchema();

    RandomBatchGenerator generator(schema);
    auto generate_time = std::chrono::steady_clock::now();
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> table, generator.Generate(RECORD_ROW_NUM));
    cout << "generate cost:" << formatTime(generate_time) << endl;

    auto write_time = std::chrono::steady_clock::now();
    write_parquet_file(*table);

    cout << "write file cost:" << formatTime(write_time) << endl;
    return arrow::Status::OK();
}

int main(int argc, char const *argv[])
{
    cout << func() << endl;
    return 0;
}
