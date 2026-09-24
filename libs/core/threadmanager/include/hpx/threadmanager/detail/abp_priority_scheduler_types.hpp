//  Copyright (c) 2026 Pratyksh Gupta
//
//  SPDX-License-Identifier: BSL-1.0
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once

#include <hpx/config.hpp>

#if defined(HPX_HAVE_CXX11_STD_ATOMIC_128BIT)

#include <hpx/modules/schedulers.hpp>

#include <mutex>

namespace hpx::threads::detail {

    /// Queue backends for `--hpx:queuing=abp-priority-{fifo,lifo}`.
    /// Must use the ABP backends (`lockfree_abp_*`), not plain
    /// `lockfree_{fifo,lifo}` (see #6793).
    using abp_priority_fifo_scheduler =
        hpx::threads::policies::local_priority_queue_scheduler<std::mutex,
            hpx::threads::policies::lockfree_abp_fifo>;

    using abp_priority_lifo_scheduler =
        hpx::threads::policies::local_priority_queue_scheduler<std::mutex,
            hpx::threads::policies::lockfree_abp_lifo>;

}    // namespace hpx::threads::detail

#endif
