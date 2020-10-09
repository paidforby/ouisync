#pragma once

#include "id.h"
#include "tagged.h"
#include "path.h"

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/filesystem/fstream.hpp>
#include <boost/filesystem/operations.hpp>

namespace ouisync::object::io {

// --- store ---------------------------------------------------------

template<class O>
inline
Id store(const fs::path& root, const O& object) {
    auto id = object.calculate_id();
    auto path = root / path::from_id(id);
    // XXX: if this probes every single directory in path, then it might be
    // slow and in such case we could instead try to create only the last 2.
    fs::create_directories(path.parent_path());
    fs::ofstream ofs(path, std::fstream::out | std::fstream::binary | std::fstream::trunc);
    if (!ofs.is_open()) throw std::runtime_error("Failed to store object");
    boost::archive::text_oarchive oa(ofs);
    tagged::Save<O> save{object};
    oa << save;
    return id;
}

// --- load ----------------------------------------------------------

template<class O>
inline
O load(const fs::path& path) {
    fs::ifstream ifs(path, fs::ifstream::binary);
    if (!ifs.is_open())
        throw std::runtime_error("Failed to open object");
    boost::archive::text_iarchive ia(ifs);
    O result;
    tagged::Load<O> loader{result};
    ia >> loader;
    return result;
}

template<class O>
inline
O load(const fs::path& objdir, const Id& id) {
    return load<O>(objdir / path::from_id(id));
}

template<class O0, class O1, class ... Os> // Two or more
inline
variant<O0, O1, Os...> load(const fs::path& objdir, const Id& id) {
    return load<variant<O0, O1, Os...>>(objdir / path::from_id(id));
}

template<class O0, class O1, class ... Os> // Two or more
inline
variant<O0, O1, Os...> load(const fs::path& path) {
    return load<variant<O0, O1, Os...>>(path);
}

// --- remove --------------------------------------------------------

bool remove(const fs::path& objdir, const Id& id);

// -------------------------------------------------------------------

} // namespace
