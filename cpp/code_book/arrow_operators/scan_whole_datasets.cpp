
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

#define DATASET_NAME "slice"

/**
 * @brief 扫描整个指定目录，获取数据类型、路径和数据表对象
 *
 * @param filesystem
 * @param format 需要扫描的文件格式
 * @param base_dir
 * @return arrow::Result<std::shared_ptr<arrow::Table>>
 */
arrow::Result<std::shared_ptr<arrow::Table>> ScanWholeDataset(
    const std::shared_ptr<arrow::fs::FileSystem> &filesystem,
    const std::shared_ptr<arrow::dataset::FileFormat> &format, const std::string &base_dir)
{
    // 通过扫描路径获取dataset
    // 我们也要传递要使用的文件系统和要用于读取的文件格式。这让我们可以选择（例如）读取本地文件或Amazon S3中的文件，或在Parquet和CSV之间进行选择。
    arrow::fs::FileSelector selector;
    selector.base_dir = base_dir;
    ARROW_ASSIGN_OR_RAISE(auto factory, arrow::dataset::FileSystemDatasetFactory::Make(filesystem, selector, format, arrow::dataset::FileSystemFactoryOptions()));
    ARROW_ASSIGN_OR_RAISE(auto dataset, factory->Finish());

    // 输出fragments，一个fragments可以代表一个数据集块？
    ARROW_ASSIGN_OR_RAISE(auto fragments, dataset->GetFragments())
    for (const auto &fragment : fragments)
    {
        std::cout << "发现 fragment: " << (*fragment)->ToString() << std::endl;
    }

    // 读取整个路径下的数据文件，并放到一张Table里
    ARROW_ASSIGN_OR_RAISE(auto scan_builder, dataset->NewScan());

    // 可以设置读取方式？ TODO 没明白为啥没作用
    // auto options = std::make_shared<arrow::dataset::ParquetFragmentScanOptions>();
    // options->arrow_reader_properties->set_read_dictionary(0, true); // 第一行是dict
    // ARROW_RETURN_NOT_OK(scan_builder->FragmentScanOptions(options));

    ARROW_ASSIGN_OR_RAISE(auto scanner, scan_builder->Finish());
    return scanner->ToTable();
}

arrow::Status func()
{
    std::shared_ptr<arrow::dataset::FileFormat> format;
    format = std::make_shared<arrow::dataset::ParquetFileFormat>(); // Parquet
    // format = std::make_shared<arrow::dataset::IpcFileFormat>(); // IPC
    
    std::string base_path{root_path + "/" + DATASET_NAME + "_output/parquet_dataset"}; // Parquet
    // std::string base_path{root_path + "/" + DATASET_NAME + "_output/feather_dataset"}; // IPC

    ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUri(uri, &root_path));

    std::shared_ptr<arrow::Table> table;
    ARROW_ASSIGN_OR_RAISE(table, ScanWholeDataset(fs, format, base_path));

    std::cout << "Read " << table->num_rows() << " rows" << std::endl;
    std::cout << table->ToString() << std::endl;
    return arrow::Status::OK();
}

int main(int argc, char const *argv[])
{
    cout << func() << endl;
    return 0;
}
