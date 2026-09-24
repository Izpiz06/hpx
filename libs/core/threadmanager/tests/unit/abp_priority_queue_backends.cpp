//  Copyright (c) 2026 Pratyksh Gupta
//
//  SPDX-License-Identifier: BSL-1.0
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// Regression guard for #6793: --hpx:queuing=abp-priority-{fifo,lifo} must
// wire the ABP queue backends, not plain lockfree_{fifo,lifo}.

#include <hpx/config.hpp>
#include <hpx/future.hpp>
#include <hpx/init.hpp>
#include <hpx/modules/schedulers.hpp>
#include <hpx/modules/testing.hpp>
#include <hpx/threadmanager/detail/abp_priority_scheduler_types.hpp>

#include <mutex>
#include <string>
#include <type_traits>
#include <vector>

#if defined(HPX_HAVE_CXX11_STD_ATOMIC_128BIT)

static_assert(!std::is_same_v<hpx::threads::policies::lockfree_abp_lifo,
                  hpx::threads::policies::lockfree_lifo>,
    "ABP LIFO backend must differ from plain LIFO");

static_assert(!std::is_same_v<hpx::threads::policies::lockfree_abp_fifo,
                  hpx::threads::policies::lockfree_fifo>,
    "ABP FIFO backend must differ from plain FIFO");

static_assert(
    std::is_same_v<hpx::threads::detail::abp_priority_lifo_scheduler,
        hpx::threads::policies::local_priority_queue_scheduler<std::mutex,
            hpx::threads::policies::lockfree_abp_lifo>>,
    "abp-priority-lifo must use lockfree_abp_lifo");

static_assert(
    std::is_same_v<hpx::threads::detail::abp_priority_fifo_scheduler,
        hpx::threads::policies::local_priority_queue_scheduler<std::mutex,
            hpx::threads::policies::lockfree_abp_fifo>>,
    "abp-priority-fifo must use lockfree_abp_fifo");

int hpx_main()
{
    bool ran = false;
    hpx::async([&ran]() { ran = true; }).get();
    HPX_TEST(ran);
    return hpx::local::finalize();
}

int main(int argc, char* argv[])
{
    std::vector<std::string> cfg = {"hpx.os_threads=2"};

    {
        // Smoke: abp-priority-lifo must start with 128-bit atomics available.
        std::vector<std::string> args = {
            argv[0], "--hpx:queuing=abp-priority-lifo"};
        std::vector<char*> av;
        for (auto& s : args)
            av.push_back(s.data());
        int const ac = static_cast<int>(av.size());

        hpx::local::init_params init_args;
        init_args.cfg = cfg;
        HPX_TEST_EQ(hpx::local::init(hpx_main, ac, av.data(), init_args), 0);
    }

    {
        std::vector<std::string> args = {
            argv[0], "--hpx:queuing=abp-priority-fifo"};
        std::vector<char*> av;
        for (auto& s : args)
            av.push_back(s.data());
        int const ac = static_cast<int>(av.size());

        hpx::local::init_params init_args;
        init_args.cfg = cfg;
        HPX_TEST_EQ(hpx::local::init(hpx_main, ac, av.data(), init_args), 0);
    }

    return hpx::util::report_errors();
}

#else

int main()
{
    // ABP priority queues require 128-bit atomics; skip on this build.
    return 0;
}

#endif
