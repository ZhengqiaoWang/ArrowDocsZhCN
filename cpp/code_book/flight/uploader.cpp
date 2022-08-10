#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <arrow/flight/api.h>
#include <arrow/filesystem/api.h>

#include <iostream>
#include <string>
using namespace std;

#define SERVER_PORT 33000
// 该文件是通过read_write_parquet生成的
#define DATA_FILE_1 "test2.parquet"
#define DATA_FILE_2 "test.parquet"

arrow::Status uploadData(std::unique_ptr<arrow::flight::FlightClient> &client)
{
    // 打开数据文件
    auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::io::RandomAccessFile> input, fs->OpenInputFile(DATA_FILE_1));

    // 构造reader用于读取
    std::unique_ptr<parquet::arrow::FileReader> reader;
    ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(std::move(input), arrow::default_memory_pool(), &reader));

    // 设置请求头（设置文件路径和元数据）
    auto descriptor = arrow::flight::FlightDescriptor::Path({DATA_FILE_1});
    std::shared_ptr<arrow::Schema> schema;
    ARROW_RETURN_NOT_OK(reader->GetSchema(&schema));

    // 启动RPC请求，获取writer和metadata_reader
    std::unique_ptr<arrow::flight::FlightStreamWriter> writer;
    std::unique_ptr<arrow::flight::FlightMetadataReader> metadata_reader;
    ARROW_ASSIGN_OR_RAISE(auto put_stream, client->DoPut(descriptor, schema));
    writer = std::move(put_stream.writer);
    metadata_reader = std::move(put_stream.reader);

    // 上传数据
    std::shared_ptr<arrow::RecordBatchReader> batch_reader; // 创建batch读取器，一次batch包含了所有rowgroups
    std::vector<int> row_groups(reader->num_row_groups());  // 保持原有的rowgroup
    std::iota(row_groups.begin(), row_groups.end(), 0);     // 获取所有rowgroups
    cout << "row groups: 0-" << row_groups.size() - 1 << endl;

    ARROW_RETURN_NOT_OK(reader->GetRecordBatchReader(row_groups, &batch_reader));
    int64_t batches = 0;
    while (true)
    {
        ARROW_ASSIGN_OR_RAISE(auto batch, batch_reader->Next()); // 每次读取一波数据
        if (!batch)
            break;
        ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch)); // writer将数据写入
        batches++;
    }

    ARROW_RETURN_NOT_OK(writer->Close());
    cout << "写了 " << batches << " batches" << std::endl;

    return arrow::Status::OK();
}

arrow::Status getData(std::unique_ptr<arrow::flight::FlightClient> &client)
{
    // 在完成写入之后，通过GetFlightInfo来获取指定descriptor文件的表结构

    auto descriptor = arrow::flight::FlightDescriptor::Path({DATA_FILE_1});

    std::unique_ptr<arrow::flight::FlightInfo> flight_info;
    ARROW_ASSIGN_OR_RAISE(flight_info, client->GetFlightInfo(descriptor));
    cout << flight_info->descriptor().ToString() << std::endl;
    cout << "=== Schema ===" << std::endl;
    std::shared_ptr<arrow::Schema> info_schema;
    arrow::ipc::DictionaryMemo dictionary_memo; // 声明从IPC到字典化的内存结构
    ARROW_ASSIGN_OR_RAISE(info_schema, flight_info->GetSchema(&dictionary_memo));
    cout << info_schema->ToString() << std::endl;
    cout << "==============" << std::endl;

    // 然后在读取数据
    std::unique_ptr<arrow::flight::FlightStreamReader> stream;
    // 有意思的是，他把flight的从目的地获取数据的过程看作坐飞机，手里需要拿个ticket，保存了目的地

    for (auto &points : flight_info->endpoints())
    {
        cout << "-----> end point:" << endl;
        cout << "ticket:" << points.ticket.ticket << endl;
        for (auto &loc : points.locations)
        {
            cout << "loc:" << loc.ToString() << endl;
        }
    }

    ARROW_ASSIGN_OR_RAISE(stream, client->DoGet(flight_info->endpoints()[0].ticket)); // 要第一个符合descriptor的文件
    std::shared_ptr<arrow::Table> table;
    ARROW_ASSIGN_OR_RAISE(table, stream->ToTable());
    arrow::PrettyPrintOptions print_options(/*indent=*/0, /*window=*/2);
    ARROW_RETURN_NOT_OK(arrow::PrettyPrint(*table, print_options, &cout));

    return arrow::Status::OK();
}

arrow::Status delData(std::unique_ptr<arrow::flight::FlightClient> &client)
{
    // flight可以调用自定义的actions，可以先获取支持的Actions
    auto actions = client->ListActions();
    cout << "=== Actions ===" << std::endl;
    for (auto &action : actions.ValueUnsafe())
    {
        cout << "action[" << action.type << "]: " << action.description << endl;
    }

    // 之后我们调用支持的drop_dataset来删除DATA_FILE_1
    arrow::flight::Action action{"drop_dataset", arrow::Buffer::FromString(DATA_FILE_1)};
    std::unique_ptr<arrow::flight::ResultStream> results;
    ARROW_ASSIGN_OR_RAISE(results, client->DoAction(action));
    cout << "Deleted dataset" << DATA_FILE_1 << std::endl;

    // 删除完成后验证一下，输出一下所有符合flight的shema，因为没有数据，所以直接退出了
    std::unique_ptr<arrow::flight::FlightListing> listing;
    ARROW_ASSIGN_OR_RAISE(listing, client->ListFlights()); // 获取flight列表
    while (true)
    {
        std::unique_ptr<arrow::flight::FlightInfo> flight_info;
        ARROW_ASSIGN_OR_RAISE(flight_info, listing->Next()); // 遍历一遍flight列表
        if (!flight_info)
            break;
        cout << flight_info->descriptor().ToString() << std::endl;
        cout << "=== Schema ===" << std::endl;
        std::shared_ptr<arrow::Schema> info_schema;
        arrow::ipc::DictionaryMemo dictionary_memo;
        ARROW_ASSIGN_OR_RAISE(info_schema, flight_info->GetSchema(&dictionary_memo));
        cout << info_schema->ToString() << std::endl;
        cout << "==============" << std::endl;
    }
    cout << "End of listing" << std::endl;

    return arrow::Status::OK();
}

arrow::Status connect()
{
    arrow::flight::Location location;
    ARROW_ASSIGN_OR_RAISE(location,
                          arrow::flight::Location::ForGrpcTcp("localhost", SERVER_PORT));

    std::unique_ptr<arrow::flight::FlightClient> client;
    ARROW_ASSIGN_OR_RAISE(client, arrow::flight::FlightClient::Connect(location));
    cout << "已连接上 " << location.ToString() << std::endl;

    ARROW_RETURN_NOT_OK(uploadData(client));
    ARROW_RETURN_NOT_OK(getData(client));
    ARROW_RETURN_NOT_OK(delData(client));

    client->Close();
    return arrow::Status::OK();
}

int main(int argc, char const *argv[])
{
    cout << connect() << endl;
    return 0;
}
