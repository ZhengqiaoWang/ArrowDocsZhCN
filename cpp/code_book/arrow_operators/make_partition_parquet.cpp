
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/filesystem/api.h>
#include <parquet/arrow/writer.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/discovery.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_ipc.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/scanner.h>

#include <iostream>
using namespace std;

#include "common.h"

#define DATASET_NAME "partition"

/**
 * @brief 创建Hive分区数据集样例
 *
 * @param filesystem
 * @param root_path
 * @return arrow::Result<std::string>
 */
arrow::Result<std::string> CreateExampleParquetHivePartitionedDataset(
    const std::shared_ptr<arrow::fs::FileSystem> &filesystem, const std::string &root_path)
{
    auto base_path = root_path + "/" + DATASET_NAME + "_output/parquet_dataset";
    ARROW_RETURN_NOT_OK(filesystem->CreateDir(base_path));
    ARROW_RETURN_NOT_OK(filesystem->DeleteDirContents(base_path)); // 删除文件夹内历史数据
    ARROW_ASSIGN_OR_RAISE(auto table, CreateTableWithPart());      // 创建表(多了一个part字段)

    // 通过dataset来写文件
    auto dataset = std::make_shared<arrow::dataset::InMemoryDataset>(table);
    ARROW_ASSIGN_OR_RAISE(auto scanner_builder, dataset->NewScan());
    ARROW_ASSIGN_OR_RAISE(auto scanner, scanner_builder->Finish());

    // partition schema说明了要按照哪个列分区，这里取part列
    auto partition_schema = arrow::schema({arrow::field("part", arrow::utf8())});
    // 我们使用Hive-style分区方式，这种方式会创建key=value形式的目录结构
    auto partitioning = std::make_shared<arrow::dataset::HivePartitioning>(partition_schema);
    // 写parquet文件
    auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();
    arrow::dataset::FileSystemDatasetWriteOptions write_options;
    write_options.file_write_options = format->DefaultWriteOptions();
    write_options.filesystem = filesystem;
    write_options.base_dir = base_path;
    write_options.partitioning = partitioning;
    write_options.basename_template = "part{i}.parquet"; // 文件名为part{i}.parquet
    ARROW_RETURN_NOT_OK(arrow::dataset::FileSystemDataset::Write(write_options, scanner));
    return base_path;
}

arrow::Status func()
{
    std::string base_path;
    std::shared_ptr<arrow::dataset::FileFormat> format;
    cout << "uri:" << uri << endl;
    ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUri(uri, &root_path));

    format = std::make_shared<arrow::dataset::ParquetFileFormat>();
    ARROW_ASSIGN_OR_RAISE(base_path, CreateExampleParquetHivePartitionedDataset(fs, root_path));

    cout << "base_path:" << base_path << endl;
    cout << "root_path:" << root_path << endl;

    return arrow::Status::OK();
}

int main(int argc, char const *argv[])
{
    cout << func() << endl;
    return 0;
}
