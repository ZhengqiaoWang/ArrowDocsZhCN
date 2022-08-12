
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <arrow/flight/api.h>
#include <arrow/filesystem/api.h>

#include <arrow/filesystem/hdfs.h>

#include <iostream>
#include <string>
#include "common.h"

using namespace std;

class ParquetStorageService : public arrow::flight::FlightServerBase
{
public:
    explicit ParquetStorageService(std::shared_ptr<arrow::fs::FileSystem> root)
        : root_(std::move(root))
    {
    }

    arrow::Status ListFlights(
        const arrow::flight::ServerCallContext &, const arrow::flight::Criteria *,
        std::unique_ptr<arrow::flight::FlightListing> *listings) override
    {
        arrow::fs::FileSelector selector;
        selector.base_dir = "/";
        ARROW_ASSIGN_OR_RAISE(auto listing, root_->GetFileInfo(selector));
        std::vector<arrow::flight::FlightInfo> flights;
        for (const auto &file_info : listing)
        {
            if (!file_info.IsFile() || file_info.extension() != "parquet")
                continue;

            ARROW_ASSIGN_OR_RAISE(auto info, MakeFlightInfo(file_info));
            flights.push_back(std::move(info));
        }

        *listings = std::unique_ptr<arrow::flight::FlightListing>(
            new arrow::flight::SimpleFlightListing(std::move(flights)));

        return arrow::Status::OK();
    }

    arrow::Status GetFlightInfo(const arrow::flight::ServerCallContext &,
                                const arrow::flight::FlightDescriptor &descriptor,
                                std::unique_ptr<arrow::flight::FlightInfo> *info) override
    {
        ARROW_ASSIGN_OR_RAISE(auto file_info, FileInfoFromDescriptor(descriptor));
        ARROW_ASSIGN_OR_RAISE(auto flight_info, MakeFlightInfo(file_info));
        *info = std::unique_ptr<arrow::flight::FlightInfo>(
            new arrow::flight::FlightInfo(std::move(flight_info)));

        return arrow::Status::OK();
    }

    arrow::Status DoGet(const arrow::flight::ServerCallContext &,
                        const arrow::flight::Ticket &request,
                        std::unique_ptr<arrow::flight::FlightDataStream> *stream) override
    {
        ARROW_ASSIGN_OR_RAISE(auto input, root_->OpenInputFile(request.ticket));
        std::unique_ptr<parquet::arrow::FileReader> reader;
        ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(std::move(input),
                                                     arrow::default_memory_pool(), &reader));
        std::shared_ptr<arrow::Table> table;
        ARROW_RETURN_NOT_OK(reader->ReadTable(&table));

        // Note that we can't directly pass TableBatchReader to
        // RecordBatchStream because TableBatchReader keeps a non-owning
        // reference to the underlying Table, which would then get freed
        // when we exit this function

        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        arrow::TableBatchReader batch_reader(*table);

        ARROW_ASSIGN_OR_RAISE(batches, batch_reader.ToRecordBatches());
        ARROW_ASSIGN_OR_RAISE(auto owning_reader, arrow::RecordBatchReader::Make(
                                                      std::move(batches), table->schema()));

        arrow::ipc::IpcWriteOptions options;
        options.allow_64bit = true;

        *stream = std::unique_ptr<arrow::flight::FlightDataStream>(
            new arrow::flight::RecordBatchStream(owning_reader, options));

        return arrow::Status::OK();
    }

private:
    arrow::Result<arrow::flight::FlightInfo> MakeFlightInfo(
        const arrow::fs::FileInfo &file_info)
    {
        ARROW_ASSIGN_OR_RAISE(auto input, root_->OpenInputFile(file_info));
        std::unique_ptr<parquet::arrow::FileReader> reader;
        ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(std::move(input),
                                                     arrow::default_memory_pool(), &reader));
        std::shared_ptr<arrow::Schema> schema;
        ARROW_RETURN_NOT_OK(reader->GetSchema(&schema));
        auto descriptor = arrow::flight::FlightDescriptor::Path({file_info.base_name()});
        arrow::flight::FlightEndpoint endpoint;
        endpoint.ticket.ticket = file_info.base_name();
        arrow::flight::Location location;

        ARROW_ASSIGN_OR_RAISE(location,
                              arrow::flight::Location::ForGrpcTcp("localhost", port()));
        endpoint.locations.push_back(location);

        int64_t total_records = reader->parquet_reader()->metadata()->num_rows();
        int64_t total_bytes = file_info.size();

        return arrow::flight::FlightInfo::Make(*schema, descriptor, {endpoint}, total_records,
                                               total_bytes);
    }

    arrow::Result<arrow::fs::FileInfo> FileInfoFromDescriptor(
        const arrow::flight::FlightDescriptor &descriptor)
    {
        if (descriptor.type != arrow::flight::FlightDescriptor::PATH)
        {
            return arrow::Status::Invalid("Must provide PATH-type FlightDescriptor");
        }
        else if (descriptor.path.size() != 1)
        {
            return arrow::Status::Invalid(
                "Must provide PATH-type FlightDescriptor with one path component");
        }

        return root_->GetFileInfo(descriptor.path[0]);
    }

    std::shared_ptr<arrow::fs::FileSystem> root_;

}; // end ParquetStorageService

arrow::Status startServer()
{
    // 创建并清空存储的数据文件目录
    auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
    ARROW_RETURN_NOT_OK(fs->CreateDir("./flight_datasets/"));
    // ARROW_RETURN_NOT_OK(fs->DeleteDirContents("./flight_datasets/"));
    auto root = std::make_shared<arrow::fs::SubTreeFileSystem>("./flight_datasets/", fs);

    // 设置flight监听IP端口
    arrow::flight::Location server_location;
    ARROW_ASSIGN_OR_RAISE(server_location,
                          arrow::flight::Location::ForGrpcTcp("0.0.0.0", SERVER_PORT));

    // 初始化
    arrow::flight::FlightServerOptions options(server_location);
    auto server = std::unique_ptr<arrow::flight::FlightServerBase>(
        new ParquetStorageService(std::move(root)));
    ARROW_RETURN_NOT_OK(server->Init(options));
    cout << "Listening on port " << server->port() << std::endl;

    // 启动服务（阻塞）
    ARROW_RETURN_NOT_OK(server->Serve());

    // 关闭服务
    ARROW_RETURN_NOT_OK(server->Shutdown());
    return arrow::Status::OK();
}

int main(int argc, char const *argv[])
{
    startServer();
    return 0;
}
