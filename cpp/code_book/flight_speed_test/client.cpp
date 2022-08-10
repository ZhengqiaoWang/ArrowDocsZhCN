#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <arrow/flight/api.h>
#include <arrow/filesystem/api.h>

#include <iostream>
#include <string>

#include "common.h"
using namespace std;


arrow::Status getData(std::unique_ptr<arrow::flight::FlightClient> &client)
{
    // 在完成写入之后，通过GetFlightInfo来获取指定descriptor文件的表结构

    auto descriptor = arrow::flight::FlightDescriptor::Path({PARQUET_FILE_NAME});

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

    ARROW_ASSIGN_OR_RAISE(stream, client->DoGet(flight_info->endpoints()[0].ticket)); // 要第一个符合descriptor的文件
    std::shared_ptr<arrow::Table> table;
    
    while(true)
    {
        ARROW_ASSIGN_OR_RAISE(auto stream_chunk, stream->Next());
        if (!stream_chunk.data)
            break;
        cout<<"!!!"<<endl;
    }
    // ARROW_ASSIGN_OR_RAISE(table, stream->ToTable());
    // arrow::PrettyPrintOptions print_options(/*indent=*/0, /*window=*/2);
    // ARROW_RETURN_NOT_OK(arrow::PrettyPrint(*table, print_options, &cout));

    return arrow::Status::OK();
}

arrow::Status connect()
{
    auto client_options = arrow::flight::FlightClientOptions::Defaults();
    // Set a very low limit at the gRPC layer to fail all calls
    client_options.generic_options.emplace_back("grpc.max_send_message_length", -1);

    arrow::flight::Location location;
    ARROW_ASSIGN_OR_RAISE(location,
                          arrow::flight::Location::ForGrpcTcp("localhost", SERVER_PORT));

    std::unique_ptr<arrow::flight::FlightClient> client;
    ARROW_ASSIGN_OR_RAISE(client, arrow::flight::FlightClient::Connect(location, client_options));
    cout << "已连接上 " << location.ToString() << std::endl;

    ARROW_RETURN_NOT_OK(getData(client));

    client->Close();
    return arrow::Status::OK();
}

int main(int argc, char const *argv[])
{
    cout << connect() << endl;
    return 0;
}
