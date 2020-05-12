// Copyright 2020 <Kondr11>

#include "DbActions.h"
DbActions::DbActions(std::string path)
    : path_(std::move(path))
{}

DbActions::FamilyDescriptorContainer DbActions::getFamilyDescriptorList()
{
    rocksdb::Options options;
    options.create_if_missing = true;
    std::vector<std::string> families;
    rocksdb::Status status = rocksdb::DB::ListColumnFamilies
    (options, path_, &families);
    std::cout<<status.ToString();
    assert(status.ok());

    FamilyDescriptorContainer descriptors;
    for (const std::string &familyName : families) {
        descriptors.emplace_back(familyName,
                                 rocksdb::ColumnFamilyOptions{});
    }
    BOOST_LOG_TRIVIAL(debug) << "Got families descriptors";

    return descriptors;
}

DbActions::FamilyHandlerContainer
DbActions::open(const DbActions::FamilyDescriptorContainer &descriptors)
{
    FamilyHandlerContainer handlers;

    std::vector<rocksdb::ColumnFamilyHandle *> pureHandlers;
    rocksdb::DB *dbRawPointer;
    rocksdb::Options options;
    options.create_if_missing = true;
    /*
    rocksdb::Status status = rocksdb::DB::Open(rocksdb::DBOptions{},
                             path_,
                             descriptors,
                             &pureHandlers,
                             &dbRawPointer);(*/
    rocksdb::Status status = rocksdb::DB::Open(options,
                                               path_,
                                               descriptors,
                                               &pureHandlers,
                                               &dbRawPointer);
    assert(status.ok());

    db_.reset(dbRawPointer);

    for (rocksdb::ColumnFamilyHandle *pointer : pureHandlers) {
        BOOST_LOG_TRIVIAL(debug) << "Got family: " << pointer->GetName();
        handlers.emplace_back(pointer);
    }

    return handlers;
}

DbActions::RowContainer DbActions::getRows(rocksdb::ColumnFamilyHandle *family)
{
    BOOST_LOG_TRIVIAL(debug) << "Rewrite family: " << family->GetName();

    boost::unordered_map<std::string, std::string> toWrite;

    std::unique_ptr<rocksdb::Iterator>
    it{db_->NewIterator(rocksdb::ReadOptions{}, family)};
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::string key = it->key().ToString();
        std::string value = it->value().ToString();
        toWrite[key] = value;

        BOOST_LOG_TRIVIAL(debug) << key << " : " << value;
    }

    if (!it->status().ok()) {
        BOOST_LOG_TRIVIAL(error) << it->status().ToString();
    }

    return toWrite;
}

void DbActions::hashRows(rocksdb::ColumnFamilyHandle *family,
                         const RowContainer::const_iterator &begin,
                         const RowContainer::const_iterator &end)
{
    for (auto it = begin; it != end; ++it) {
        auto &&[key, value] = *it;

        std::string toHash = key;
        toHash += ":" + value;
        std::string hash = picosha2::hash256_hex_string(toHash);

        rocksdb::Status status = db_->Put(rocksdb::WriteOptions(),
                                 family,
                                 key,
                                 hash);
        assert(status.ok());

        BOOST_LOG_TRIVIAL(info) << "Hashed from '"
        << family->GetName() << "': " << key;
        BOOST_LOG_TRIVIAL(debug) << "Put: " << key << " : " << hash;
    }
}

void DbActions::create()
{
    removeDirectoryIfExists(path_);

    rocksdb::Options options;
    options.create_if_missing = true;

    rocksdb::DB *dbRawPointer;
    rocksdb::Status status = rocksdb::DB::Open(options, path_, &dbRawPointer);
    assert(status.ok());

    db_.reset(dbRawPointer);
}
