// Copyright 2020 <Kondr11>

#include "DbActions.h"

DbActions::DbActions(std::string path)
    : path_(std::move(path))
{}

DbActions::FamilyDescriptorContainer DbActions::getFamilyDescriptorList()
{
    rocksdb::Options options;

    std::vector<std::string> families;
    rocksdb::Status status = rocksdb::DB::ListColumnFamilies
    (rocksdb::DBOptions(), path_, &families);

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

    rocksdb::Status status = rocksdb::DB::Open(rocksdb::DBOptions{},
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

void DbActions::randomFill()
{
    auto families = randomFillFamilies();
    randomFillRows(families);
}

DbActions::FamilyContainer DbActions::randomFillFamilies()
{
    static std::mt19937 generator{std::random_device{}()};
    static std::uniform_int_distribution<size_t> randomFamilyAmount{1, 5};

    size_t familyAmount = randomFamilyAmount(generator);
    FamilyContainer families{};
    for (size_t i = 0; i < familyAmount; i++) {
        static const size_t FAMILY_NAME_LENGTH = 5;

        rocksdb::ColumnFamilyHandle *familyRawPointer;
        std::string familyName = createRandomString(FAMILY_NAME_LENGTH);

        rocksdb::Status status = db_->CreateColumnFamily
        (rocksdb::ColumnFamilyOptions(),
        createRandomString(FAMILY_NAME_LENGTH),
        &familyRawPointer);
        assert(status.ok());

        families.emplace_back(familyRawPointer);

        BOOST_LOG_TRIVIAL(info) << "Create family: " << familyName;
    }

    return families;
}

void DbActions::randomFillRows(const DbActions::FamilyContainer &container)
{
    static std::mt19937 generator{std::random_device{}()};
    static std::uniform_int_distribution<size_t> randomRowAmount{5, 25};

    static const size_t KEY_LENGTH = 3;
    static const size_t VALUE_LENGTH = 8;

    size_t defaultRowAmount = randomRowAmount(generator);

    BOOST_LOG_TRIVIAL(debug) << "Fill family: default";
    for (size_t i = 0; i < defaultRowAmount; i++) {
        std::string key = createRandomString(KEY_LENGTH);
        std::string value = createRandomString(VALUE_LENGTH);

        rocksdb::Status status = db_->Put(rocksdb::WriteOptions(),
                                 key,
                                 value);
        assert(status.ok());

        BOOST_LOG_TRIVIAL(debug) << key << " : " << value;
    }

    for (const std::unique_ptr<rocksdb::ColumnFamilyHandle>
    &family : container) {
        BOOST_LOG_TRIVIAL(debug) << "Fill family: "
        << family->GetName();

        size_t rowAmount = randomRowAmount(generator);
        for (size_t i = 0; i < rowAmount; i++) {
            std::string key = createRandomString(KEY_LENGTH);
            std::string value = createRandomString(VALUE_LENGTH);

            rocksdb::Status status =
            db_->Put(rocksdb::WriteOptions(),
                                     family.get(),
                                     key,
                                     value);
            assert(status.ok());


            BOOST_LOG_TRIVIAL(debug) << key << " : " << value;
        }
    }
}
