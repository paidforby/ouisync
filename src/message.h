#pragma once

#include "shortcuts.h"
#include "cancel.h"
#include "user_id.h"
#include "version_vector.h"
#include "object/id.h"
#include "variant.h"

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/awaitable.hpp>

namespace ouisync {

struct RqBranchList {
    template<class Archive>
    void serialize(Archive&, const unsigned int) {}
};

struct RsBranchList : std::vector<UserId> {
    using std::vector<UserId>::vector;

    template<class Archive>
    void serialize(Archive& ar, const unsigned int) {
        ar & static_cast<std::vector<UserId>&>(*this);
    }
};

struct RqBranch {
    UserId branch_id;

    template<class Archive>
    void serialize(Archive& ar, const unsigned int) {
        ar & branch_id;
    }
};

struct RsBranch {
    VersionVector version_vector;
    object::Id root_id;

    template<class Archive>
    void serialize(Archive& ar, const unsigned int) {
        ar & version_vector;
        ar & root_id;
    }
};

namespace MessageDetail {
    using MessageVariant
        = variant<
            RqBranchList,
            RsBranchList,
            RqBranch,
            RsBranch
        >;
} // MessageDetail namespace

struct Message : MessageDetail::MessageVariant
{
    using MessageVariant = MessageDetail::MessageVariant;

    static
    net::awaitable<Message> receive(net::ip::tcp::socket&, Cancel);

    static
    net::awaitable<void> send(net::ip::tcp::socket&, const Message&, Cancel);

    template<class Archive>
    void serialize(Archive& ar, const unsigned int) {
        ar & static_cast<MessageVariant&>(*this);
    }
};

std::ostream& operator<<(std::ostream&, const RqBranchList&);
std::ostream& operator<<(std::ostream&, const RsBranchList&);
std::ostream& operator<<(std::ostream&, const RqBranch&);
std::ostream& operator<<(std::ostream&, const RsBranch&);
std::ostream& operator<<(std::ostream&, const Message&);

} // namespace