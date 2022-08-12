
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/io/file.h>
#include <parquet/stream_writer.h>
#include <parquet/stream_reader.h>
#include <iostream>
using namespace std;

/**
 * @brief 构造Schema结构
 *
 * @param schema
 */
void setSchema(std::shared_ptr<parquet::schema::GroupNode> &schema)
{
    // 函数中各个类型符合以下转换关系
    // NodeVector
    // |-- Node
    // |-- Node
    // |-- ...
    // |-- Node
    //
    // GroupNode::Make(_,_,NodeVector) 即 将NodeVector转换为GroupNode
    parquet::schema::NodeVector fields;

    // Make函数(列名, 可选项, parquet存储的类型 ,使用时需转换成的类型, 存储参数) // TODO 需要验证
    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "string_field", parquet::Repetition::OPTIONAL, parquet::Type::BYTE_ARRAY,
        parquet::ConvertedType::UTF8));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "char_field", parquet::Repetition::REQUIRED, parquet::Type::FIXED_LEN_BYTE_ARRAY,
        parquet::ConvertedType::NONE, 1));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "char[4]_field", parquet::Repetition::REQUIRED, parquet::Type::FIXED_LEN_BYTE_ARRAY,
        parquet::ConvertedType::NONE, 4));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "int8_field", parquet::Repetition::REQUIRED, parquet::Type::INT32,
        parquet::ConvertedType::INT_8));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "uint16_field", parquet::Repetition::REQUIRED, parquet::Type::INT32,
        parquet::ConvertedType::UINT_16));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "int32_field", parquet::Repetition::REQUIRED, parquet::Type::INT32,
        parquet::ConvertedType::INT_32));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "uint64_field", parquet::Repetition::OPTIONAL, parquet::Type::INT64,
        parquet::ConvertedType::UINT_64)); // 内部以INT64存储，使用时按照UINT64使用

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "double_field", parquet::Repetition::REQUIRED, parquet::Type::DOUBLE,
        parquet::ConvertedType::NONE));

    // User defined timestamp type.
    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "timestamp_field", parquet::Repetition::REQUIRED, parquet::Type::INT64,
        parquet::ConvertedType::TIMESTAMP_MICROS));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "chrono_milliseconds_field", parquet::Repetition::REQUIRED, parquet::Type::INT64,
        parquet::ConvertedType::TIMESTAMP_MILLIS));

    schema = std::static_pointer_cast<parquet::schema::GroupNode>(parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));
}

void writeData(parquet::StreamWriter &os)
{
    char char4_array[] = "XYZ";
    int row_max = 10;
    for (int i = 0; i < row_max; ++i)
    {
        os << std::string("string_field:") + std::to_string('a' + i % 26);
        os << static_cast<char>('a' + i % 26);
        os << char4_array;
        os << static_cast<int8_t>(i % 256);
        os << static_cast<uint16_t>(10 * i);
        os << static_cast<int32_t>(-100 * i);
        os << static_cast<uint64_t>(100 * i);
        os << 1.1 * i;
        os << std::chrono::microseconds{(3 * i) * 1000000 + i}; // timestamp
        os << std::chrono::milliseconds{(3 * i) * 1000ull + i};
        os << parquet::EndRow;

        if (i == row_max / 2)
        {
            os << parquet::EndRowGroup;
        }
    }
    std::cout << "Parquet Stream Writing complete. rows: " << os.current_row() << std::endl;
}

arrow::Status writeFile()
{
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_ASSIGN_OR_THROW(
        outfile,
        arrow::io::FileOutputStream::Open("test.parquet"));
    parquet::WriterProperties::Builder builder; // 这里使用了默认配置

    std::shared_ptr<parquet::schema::GroupNode> schema; // 注意此处是parquet的schema

    setSchema(schema);

    parquet::StreamWriter os{
        parquet::ParquetFileWriter::Open(outfile, schema, builder.build())};
    writeData(os);

    return arrow::Status::OK();
}

void readFile()
{
    std::shared_ptr<arrow::io::ReadableFile> infile;

    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open("test.parquet"));
    parquet::StreamReader os{parquet::ParquetFileReader::Open(infile)};

    // 定义读取数据类型
    parquet::StreamReader::optional<std::string> opt_string; // 注意该选项可选
    char ch;
    char char_array[4];
    int8_t int8;
    uint16_t uint16;
    int32_t int32;
    parquet::StreamReader::optional<uint64_t> opt_uint64;
    double d;
    std::chrono::microseconds ts_user;
    std::chrono::milliseconds ts_ms;

    int i;
    for (i = 0; !os.eof(); ++i)
    {
        os >> opt_string;
        os >> ch;
        os >> char_array;
        os >> int8;
        os >> uint16;
        os >> int32;
        os >> opt_uint64;
        os >> d;
        os >> ts_user;
        os >> ts_ms;
        os >> parquet::EndRow;

        std::cout << *opt_string << " ";
        std::cout << ch << " ";
        std::cout << char_array << " ";
        std::cout << int8 << " ";
        std::cout << uint16 << " ";
        std::cout << int32 << " ";
        std::cout << *opt_uint64 << " ";
        std::cout << d << " ";
        std::cout << ts_user.count() << " ";
        std::cout << ts_ms.count() << " ";
        std::cout << std::endl;
    }

    std::cout << std::endl
              << "Total rows:" << i << std::endl;
}

int main(int argc, char const *argv[])
{
    writeFile();
    readFile();
    return 0;
}
