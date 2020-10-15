#pragma once

#include "object/id.h"
#include "user_id.h"
#include "version_vector.h"
#include "namespaces.h"

#include <boost/filesystem/path.hpp>
#include <boost/optional.hpp>

namespace cpputils {
    class Data;
}

namespace ouisync {

class Branch {
public:
    using Data = cpputils::Data;

public:
    static Branch load_or_create(
            const fs::path& branchdir, const fs::path& objdir, UserId);

    const object::Id& root_object_id() const {
        return _root_id;
    }

    void root_object_id(const object::Id& id);

    bool maybe_store(const fs::path&, const Data&);

private:
    Branch(const fs::path& file_path, const fs::path& objdir,
            const UserId& user_id, const object::Id& root_id,
            VersionVector clock);

    void store();

private:
    fs::path _file_path;
    fs::path _objdir;
    UserId _user_id;
    object::Id _root_id;
    VersionVector _clock;
};

} // namespace