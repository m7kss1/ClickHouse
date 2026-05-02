#include "config.h"

#if USE_VORTEX

#include <Common/Exception.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>
#include <vortex.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class StorageVortex final : public IStorage
{
public:
    StorageVortex(
        const StorageID & table_id_,
        ColumnsDescription columns_description_,
        ConstraintsDescription constraints_,
        const String & comment)
        : IStorage(table_id_)
    {
        StorageInMemoryMetadata storage_metadata;
        storage_metadata.setColumns(columns_description_);
        storage_metadata.setConstraints(constraints_);
        storage_metadata.setComment(comment);
        setInMemoryMetadata(storage_metadata);
    }

    String getName() const override { return "Vortex"; }
};

void registerStorageVortex(StorageFactory & factory)
{
    [[maybe_unused]] auto * vortex_ffi_link_check = &vx_session_new;

    factory.registerStorage("Vortex", [](const StorageFactory::Arguments & args)
    {
        if (!args.engine_args.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Engine {} doesn't support any arguments ({} given)",
                args.engine_name, args.engine_args.size());

        return std::make_shared<StorageVortex>(args.table_id, args.columns, args.constraints, args.comment);
    });
}

}

#endif
