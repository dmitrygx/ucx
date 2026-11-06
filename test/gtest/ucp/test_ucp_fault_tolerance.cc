/**
* Copyright (c) NVIDIA CORPORATION & AFFILIATES, 2025. ALL RIGHTS RESERVED.
*
* See file LICENSE for terms.
*/

#include "test_ucp_memheap.h"
#include <common/test_helpers.h>
#include <algorithm>
#include <string>

extern "C" {
#include <ucp/core/ucp_am.h>
#include <ucp/core/ucp_ep.h>
#include <ucp/core/ucp_ep.inl>
#include <ucp/core/ucp_context.h>
#include <ucp/core/ucp_rkey.h>
#include <ucp/core/ucp_worker.inl>
#include <ucp/proto/proto.h>
#include <ucp/proto/proto_debug.h>
#include <ucp/proto/proto_select.inl>
#include <ucp/wireup/address.h>
#include <ucp/wireup/wireup.h>
#include <ucp/wireup/wireup_ep.h>
#include <uct/base/uct_iface.h>
#if HAVE_IB
#include <uct/ib/ud/base/ud_iface.h>
#endif
}

/**
 * Test class for fault tolerance with injected failures
 */
class test_ucp_fault_tolerance : public test_ucp_memheap {
public:
    static void get_test_variants(std::vector<ucp_test_variant>& variants) {
        static constexpr unsigned msg_size_variants[] = {
            TEST_MSG_SIZE_SMALL, TEST_MSG_SIZE_MEDIUM, TEST_MSG_SIZE_LARGE
        };
        static constexpr unsigned rma_ops[]           = {
            TEST_OP_PUT, TEST_OP_GET
        };
        static constexpr unsigned am_ops[]            = {
            TEST_OP_AM, TEST_OP_AM | TEST_OP_ALL_LANES_FAILED,
            TEST_OP_AM | TEST_OP_FLUSH
        };
        static constexpr unsigned am_msg_size_variants[] = {
            TEST_MSG_SIZE_SMALL, TEST_MSG_SIZE_MEDIUM
        };

        for (unsigned rma_op : rma_ops) {
            for (unsigned msg_size_variant : msg_size_variants) {
                add_variant_with_value(variants, UCP_FEATURE_RMA,
                                       rma_op | msg_size_variant,
                                       op_name(rma_op | msg_size_variant));
            }

            add_variant_with_value(variants, UCP_FEATURE_RMA,
                                   rma_op | TEST_OP_FLUSH,
                                   op_name(rma_op | TEST_OP_FLUSH));
        }

        for (unsigned am_op : am_ops) {
            for (unsigned msg_size_variant : am_msg_size_variants) {
                add_variant_with_value(variants, UCP_FEATURE_AM,
                                       am_op | msg_size_variant,
                                       op_name(am_op | msg_size_variant));
            }
        }

        add_variant_with_value(variants, UCP_FEATURE_AM | UCP_FEATURE_RMA,
                               TEST_OP_PUT | TEST_OP_AM | TEST_OP_FLUSH,
                               op_name(TEST_OP_PUT |TEST_OP_AM | TEST_OP_FLUSH));
    }

    test_ucp_fault_tolerance() {
        configure_peer_failure_settings();
        // reduce UD testing time 
        modify_config("KEEPALIVE_INTERVAL", "0.3s");
    }

protected:
    static constexpr uint16_t AM_ID = 0;

    enum {
        GOOD_EP_INDEX = 0,      /* Index for good endpoint */
        INJECTED_EP_INDEX = 1   /* Index for failure-injected endpoint */
    };

    enum failure_side_t {
        FAILURE_SIDE_INITIATOR, /* Inject failure on sender (initiator) side */
        FAILURE_SIDE_TARGET     /* Inject failure on receiver (target) side */
    };

    enum test_op_t {
        TEST_OP_PUT              = UCS_BIT(0),
        TEST_OP_GET              = UCS_BIT(1),
        TEST_OP_AM               = UCS_BIT(2),
        TEST_OP_FLUSH            = UCS_BIT(3),
        TEST_OP_ALL_LANES_FAILED = UCS_BIT(4),
        TEST_MSG_SIZE_SMALL      = UCS_BIT(5),
        TEST_MSG_SIZE_MEDIUM     = UCS_BIT(6),
        TEST_MSG_SIZE_LARGE      = UCS_BIT(7),
        TEST_MSG_SIZE_MASK       = TEST_MSG_SIZE_SMALL | TEST_MSG_SIZE_MEDIUM |
                                   TEST_MSG_SIZE_LARGE
    };

    /* Must stay below cap.get.min_zcopy, so GET cannot use zcopy */
    static constexpr size_t SMALL_MSG_SIZE  = 1;
    static constexpr size_t MEDIUM_MSG_SIZE = UCS_KBYTE;
    static constexpr size_t LARGE_MSG_SIZE  = 100 * UCS_MBYTE;

    void init() override {
        if (get_variant_value() & TEST_OP_ALL_LANES_FAILED) {
            modify_config("RECOVERY_RETRIES", "1");
            modify_config("KEEPALIVE_INTERVAL", std::to_string(3) + "s");
        }

        ucp_test::init();

        ucp_ep_params_t ep_params = get_ep_params();
        sender().connect(&receiver(), ep_params, GOOD_EP_INDEX);
        sender().connect(&receiver(), ep_params, INJECTED_EP_INDEX);
        receiver().connect(&sender(), ep_params, GOOD_EP_INDEX);
        receiver().connect(&sender(), ep_params, INJECTED_EP_INDEX);

        if (get_variant_value() & TEST_OP_AM) {
            set_am_handler();
        }
    }

    void cleanup() override {
        test_ucp_memheap::cleanup();

        m_probe_mock      = NULL;
        m_probe_count     = 0;
        m_held_probe_comp = NULL;
    }

    void set_am_handler() {
        ucp_am_handler_param_t param;
        param.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                           UCP_AM_HANDLER_PARAM_FIELD_CB |
                           UCP_AM_HANDLER_PARAM_FIELD_ARG;
        param.id         = AM_ID;
        param.cb         = am_recv_cb;
        param.arg        = reinterpret_cast<void*>(this);

        ucs_status_t status = ucp_worker_set_am_recv_handler(receiver().worker(), &param);
        ASSERT_UCS_OK(status);
    }

    static ucs_status_t am_recv_cb(void *arg, const void *header,
                                   size_t header_length, void *data,
                                   size_t length,
                                   const ucp_am_recv_param_t *param) {
        test_ucp_fault_tolerance *self =
            reinterpret_cast<test_ucp_fault_tolerance*>(arg);

        /* Rendezvous passes a descriptor, not the payload. Inline and
         * persistent receives both point at the payload for this callback. */
        if (param->recv_attr & UCP_AM_RECV_ATTR_FLAG_RNDV) {
            ADD_FAILURE() << "RNDV is not covered yet";
            return UCS_OK;
        }

        self->m_am_rbuf.resize(length);
        memcpy(self->m_am_rbuf.data(), data, length);
        self->m_am_received = true;

        return UCS_OK;
    }

    /**
     * Get endpoint parameters with optional failure injection flag
     */
    ucp_ep_params_t get_ep_params() override {
        ucp_ep_params_t params = test_ucp_memheap::get_ep_params();

        params.field_mask     |= UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE |
                                 UCP_EP_PARAM_FIELD_ERR_HANDLER;
        params.err_mode        = UCP_ERR_HANDLING_MODE_FAILOVER;
        params.err_handler.cb  = err_cb;
        params.err_handler.arg = reinterpret_cast<void*>(this);

        return params;
    }

    /**
     * Error callback for endpoint failures
     */
    static void err_cb(void *arg, ucp_ep_h ep, ucs_status_t status) {
        test_ucp_fault_tolerance *self =
            reinterpret_cast<test_ucp_fault_tolerance*>(arg);
        ucp_ep_h sender_ep = self->sender().ep(0, INJECTED_EP_INDEX);

        UCS_TEST_MESSAGE << "Error callback invoked: " << ucs_status_string(status);

        EXPECT_TRUE((UCS_ERR_CONNECTION_RESET == status) ||
                    (UCS_ERR_ENDPOINT_TIMEOUT == status) ||
                    (UCS_ERR_CANCELED == status));

        self->m_err_status = status;
        ++self->m_total_err_count;
        if (ep == sender_ep) {
            ++self->m_initiator_err_count;
        }
    }

    static void shuffle_lanes(std::vector<ucp_lane_index_t> &lanes, const std::string &lane_type) {
        if (lanes.size() < 2) {
            UCS_TEST_SKIP_R("At least 2 " + lane_type + "lanes are required, but only " + std::to_string(lanes.size()) +
                            " available");
        }

        std::random_shuffle(lanes.begin(), lanes.end(), ucs::rand_range);
        for (ucp_lane_index_t lane : lanes) {
            UCS_TEST_MESSAGE << lane_type << ": " << size_t(lane) << "/" << lanes.size();
        }
    }

    ucp_ep_h get_ucp_ep_for_err_injection(failure_side_t failure_side) {
        return (failure_side == FAILURE_SIDE_INITIATOR) ? sender().ep(0, INJECTED_EP_INDEX) :
               receiver().ep(0, INJECTED_EP_INDEX);
    }

    std::vector<ucp_lane_index_t> get_lanes(unsigned op_mask) {
        std::set<ucp_lane_index_t> tmp_lanes;
        std::string lane_type_str;
        unsigned lane_types;
        const ucp_lane_index_t *lane_idx;
        const ucp_lane_index_t *lanes_key_p;

        unsigned lane_type_mask = 0;
        if (op_mask & (TEST_OP_PUT | TEST_OP_GET)) {
            lane_type_mask |= UCS_BIT(UCP_LANE_TYPE_RMA_BW);
        }

        if (op_mask & TEST_OP_AM) {
            lane_type_mask |= UCS_BIT(UCP_LANE_TYPE_AM_BW);
        }

        if (op_mask & (TEST_OP_PUT | TEST_OP_GET)) {
            lane_type_str  += "RMA BW ";
            lanes_key_p = ucp_ep_config(sender().ep(0, INJECTED_EP_INDEX))->key.rma_bw_lanes;
            ucs_carray_for_each(lane_idx, lanes_key_p, UCP_MAX_LANES) {
                if (*lane_idx == UCP_NULL_LANE) {
                    continue;
                }

                lane_types = ucp_ep_config(sender().ep(0, INJECTED_EP_INDEX))->key.lanes[*lane_idx].lane_types;
                if (ucs_test_all_flags(lane_types, lane_type_mask)) {
                    tmp_lanes.insert(*lane_idx);
                }
            }
        }

        if (op_mask & TEST_OP_AM) {
            lane_type_mask |= UCS_BIT(UCP_LANE_TYPE_AM_BW);
            lane_type_str  += "AM BW ";
            lanes_key_p = ucp_ep_config(sender().ep(0, INJECTED_EP_INDEX))->key.am_bw_lanes;
            ucs_carray_for_each(lane_idx, lanes_key_p, UCP_MAX_LANES) {
                if (*lane_idx == UCP_NULL_LANE) {
                    continue;
                }

                lane_types = ucp_ep_config(sender().ep(0, INJECTED_EP_INDEX))->key.lanes[*lane_idx].lane_types;
                if (ucs_test_all_flags(lane_types, lane_type_mask)) {
                    tmp_lanes.insert(*lane_idx);
                }
            }
        }

        std::vector<ucp_lane_index_t> lanes(tmp_lanes.begin(), tmp_lanes.end());
        shuffle_lanes(lanes, lane_type_str);
        check_proto_coverage(op_mask);
        return lanes;
    }

    void dump_proto_select_elem(ucp_worker_cfg_index_t rkey_cfg_index,
                                const ucp_proto_select_param_t *select_param,
                                const ucp_proto_select_elem_t *select_elem) {
        ucs_string_buffer_t strb = UCS_STRING_BUFFER_INITIALIZER;
        char *line;

        ucp_proto_select_elem_info(sender().worker(),
                                   sender().ep(0, INJECTED_EP_INDEX)->cfg_index,
                                   rkey_cfg_index, select_param, select_elem, 1,
                                   0, &strb);
        ucs_string_buffer_for_each_token(line, &strb, "\n") {
            UCS_TEST_MESSAGE << line;
        }
        ucs_string_buffer_cleanup(&strb);
    }

    /**
     * Walk the selected message size ranges of 'op_id' and verify that every
     * range is served by a real protocol. A range left to the 'reconfig' stub
     * protocol can never be completed on this endpoint configuration.
     */
    void check_op_proto_coverage(ucp_operation_id_t op_id,
                                 ucp_proto_select_t *proto_select,
                                 ucp_worker_cfg_index_t rkey_cfg_index) {
        ucp_worker_h worker = sender().worker();
        ucp_proto_select_param_t select_param;
        ucp_proto_select_elem_t *select_elem;
        ucp_proto_query_attr_t query_attr;
        ucp_memory_info_t mem_info;
        size_t range_start;
        bool gap_found;

        ucp_memory_info_set_host(&mem_info);
        ucp_proto_select_param_init(&select_param, op_id, 0, 0,
                                    UCP_DATATYPE_CONTIG, &mem_info, 1);

        select_elem = ucp_proto_select_lookup_slow(
                worker, proto_select, 0,
                sender().ep(0, INJECTED_EP_INDEX)->cfg_index, rkey_cfg_index,
                &select_param);
        if (select_elem == nullptr) {
            ADD_FAILURE() << ucp_operation_names[op_id]
                          << ": protocol selection is not initialized";
            return;
        }

        gap_found   = false;
        range_start = 0;
        do {
            if (!ucp_proto_select_elem_query(worker, select_elem, range_start,
                                             &query_attr)) {
                ADD_FAILURE() << ucp_operation_names[op_id]
                              << ": no protocol for message sizes "
                              << range_start << ".."
                              << query_attr.max_msg_length;
                gap_found = true;
            }

            range_start = query_attr.max_msg_length + 1;
        } while (query_attr.max_msg_length != SIZE_MAX);

        if (gap_found) {
            dump_proto_select_elem(rkey_cfg_index, &select_param, select_elem);
        }
    }

    /**
     * Verify that the operations exercised by the current variant have no
     * message size range without a protocol.
     */
    void check_proto_coverage(unsigned op_mask) {
        ucp_ep_h ep = sender().ep(0, INJECTED_EP_INDEX);
        ucp_worker_cfg_index_t rkey_cfg_index;
        ucp_proto_select_t *proto_select;

        if (op_mask & TEST_OP_AM) {
            check_op_proto_coverage(UCP_OP_ID_AM_SEND,
                                    &ucp_ep_config(ep)->proto_select,
                                    UCP_WORKER_CFG_INDEX_NULL);
        }

        if (!(op_mask & (TEST_OP_PUT | TEST_OP_GET))) {
            return;
        }

        /* RMA protocols are selected per remote key configuration */
        mapped_buffer rbuf(1, receiver());
        ucs::handle<ucp_rkey_h> rkey = rbuf.rkey(sender());

        proto_select = ucp_proto_select_get(sender().worker(), ep->cfg_index,
                                            rkey->cfg_index, &rkey_cfg_index);
        ASSERT_NE(nullptr, proto_select) << "no rkey protocol selection";

        if (op_mask & TEST_OP_PUT) {
            check_op_proto_coverage(UCP_OP_ID_PUT, proto_select,
                                    rkey_cfg_index);
        }

        if (op_mask & TEST_OP_GET) {
            check_op_proto_coverage(UCP_OP_ID_GET, proto_select,
                                    rkey_cfg_index);
        }
    }

    /**
     * Common helper function to test PUT, AM and FLUSH operations with injected failure
     */
    void test_put_am_flush_with_injected_failure(failure_side_t failure_side, unsigned op_mask) {
        const std::string op_str = op_name(op_mask);

        /* TODO: cover case when wireup is in progress, flush here is to complete wireup */
        flush_workers();

        std::vector<ucp_lane_index_t> lanes = get_lanes(op_mask);

        size_t size = rma_msg_size();
        mem_buffer lbuf(size, UCS_MEMORY_TYPE_HOST);
        mapped_buffer rbuf(size, receiver());
        ucs::handle<ucp_rkey_h> rkey = rbuf.rkey(sender());

        ucp_ep_h ucp_ep_for_injection = get_ucp_ep_for_err_injection(failure_side);
        for (size_t lane_idx = 0; lane_idx < lanes.size() - 1; ++lane_idx) {
            std::vector<ucs_status_ptr_t> status_ptrs;
            ucp_lane_index_t lane = lanes[lane_idx];
            uct_ep_h uct_ep_for_injection = ucp_ep_get_lane(ucp_ep_for_injection, lane);
            ucs_status_t status = uct_ep_invalidate(uct_ep_for_injection, 0);
            if (status == UCS_ERR_UNSUPPORTED) {
                UCS_TEST_SKIP_R("uct_ep_invalidate is not supported");
            }

            EXPECT_EQ(UCS_OK, status) << "uct_ep_invalidate returned status: "
                                      << ucs_status_string(status);

            UCS_TEST_MESSAGE << "Attempting " << op_str
                             << " operation after failure injection on lane "
                             << size_t(lane) << '/' << lanes.size() << "...";

            m_am_received = false;
            m_am_rbuf.clear();
            status_ptrs.push_back(
                    ucp_put_nbx(sender().ep(0, INJECTED_EP_INDEX), lbuf.ptr(), size,
                    uintptr_t(rbuf.ptr()), rkey, &m_req_empty_param));
            status_ptrs.push_back(
                    ucp_am_send_nbx(sender().ep(0, INJECTED_EP_INDEX), AM_ID, NULL, 0,
                                       lbuf.ptr(), am_msg_size(), &m_req_empty_param));
            status_ptrs.push_back(
                    ucp_ep_flush_nbx(sender().ep(0, INJECTED_EP_INDEX), &m_req_empty_param));

            status = requests_wait(status_ptrs);
            EXPECT_EQ(UCS_OK, status) << "PUT, AM and FLUSH operations completed with status: "
                                      << ucs_status_string(status);
            wait_for_value(&m_am_received, true);
            EXPECT_TRUE(m_am_received) << "the AM did not arrive";

            // Check that no other lanes have been affected
            for (ucp_lane_index_t valid_lane = lane_idx + 1; valid_lane < lanes.size();
                 ++valid_lane) {
                const ucp_ep_config_t *ep_config = ucp_ep_config(sender().ep(0, INJECTED_EP_INDEX));
                ASSERT_FALSE(UCS_BIT(UCP_LANE_TYPE_FAILED) &
                             ep_config->key.lanes[lanes[valid_lane]].lane_types)
                    << "Lane " << size_t(valid_lane) << " has being marked as failed after "
                    << "failure injection on lane " << size_t(lane);
            }
        }

        short_progress_loop();
        ASSERT_EQ(0, m_total_err_count) << "Error callback invoked " << m_total_err_count << " times";
        UCS_TEST_MESSAGE << "Success";
    }

    /**
     * Common helper function to test AM send with injected failure
     */
    void test_am_with_injected_failure(failure_side_t failure_side, unsigned op_mask) {
        const std::string op_str = op_name(op_mask);

        /* TODO: cover case when wireup is in progress, flush here is to complete wireup */
        flush_workers();

        std::vector<ucp_lane_index_t> am_bw_lanes = get_lanes(op_mask);

        UCS_TEST_MESSAGE << "Attempting " << op_str << " operation before failure injection...";
        ucs_status_t status = do_am_send_and_wait(sender().ep(0, INJECTED_EP_INDEX), am_msg_size(),
                                                  op_mask & TEST_OP_FLUSH);
        EXPECT_EQ(UCS_OK, status) << op_str << " operation returned status: "
                                  << ucs_status_string(status);

        ucp_ep_h ucp_ep_for_injection = get_ucp_ep_for_err_injection(failure_side);
        for (size_t num_lanes_to_fail = (op_mask & TEST_OP_ALL_LANES_FAILED) ? am_bw_lanes.size() :
                                        (am_bw_lanes.size() - 1),
             lane_idx = 0; lane_idx < num_lanes_to_fail; ++lane_idx) {
            ucp_lane_index_t lane = am_bw_lanes[lane_idx];
            uct_ep_h uct_ep_for_injection = ucp_ep_get_lane(ucp_ep_for_injection, lane);
            const bool last_lane = (lane_idx == (am_bw_lanes.size() - 1));
            if (last_lane && has_any_transport({"ud_v", "ud_x"}) &&
                (failure_side == FAILURE_SIDE_INITIATOR)) {
                /* TODO: remove this once UD ep purge assertions are fixed */
                UCS_TEST_MESSAGE << "Keep 1 live lane for UD transports since "
                                 << "local error injection on all lanes leads to "
                                 << "failed assertion in ud_ep_purge";
                break;
            }

            status = uct_ep_invalidate(uct_ep_for_injection, 0);
            if (status == UCS_ERR_UNSUPPORTED) {
                UCS_TEST_SKIP_R("uct_ep_invalidate is not supported");
            }

            EXPECT_EQ(UCS_OK, status) << "uct_ep_invalidate returned status: "
                                      << ucs_status_string(status);

            UCS_TEST_MESSAGE << "Attempting " << op_str
                             << " operation after failure injection on lane "
                             << size_t(lane) << '/' << am_bw_lanes.size() << "...";

            std::unique_ptr<scoped_log_handler> slh;
            if (last_lane) {
                slh.reset(new scoped_log_handler(hide_errors_logger));
            }

            status = do_am_send_and_wait(sender().ep(0, INJECTED_EP_INDEX), am_msg_size(),
                                         op_mask & TEST_OP_FLUSH);
            if (!last_lane) {
                EXPECT_EQ(UCS_OK, status) << op_str << " operation returned status: "
                                          << ucs_status_string(status);
                ASSERT_EQ(0, m_total_err_count) << "Error callback invoked " << m_total_err_count << " times";
            } else if ((failure_side == FAILURE_SIDE_TARGET) &&
                       has_transport("dc_x")) {
                /* DC cannot detect remote DCI failure (connect2iface); test limitation. */
            } else if (status == UCS_OK) {
                /* Some lanes recovered; EP still operable, no error callback required. */
            } else {
                /* Operation failed => EP must fail with exactly one initiator err CB. */
                ucs_time_t deadline = ucs::get_deadline();
                while ((m_initiator_err_count == 0) &&
                       (ucs_get_time() < deadline)) {
                    short_progress_loop();
                }

                ASSERT_EQ(1, m_initiator_err_count)
                        << "Error callback invoked " << m_initiator_err_count
                        << " times";
                /* Remote may detect failure via KA/control msgs, at most once. */
                ASSERT_LE(m_total_err_count - m_initiator_err_count, 1)
                        << "Error callback invoked " << m_total_err_count
                        << " times";
            }
        }

        UCS_TEST_MESSAGE << "Success";
    }

    /**
     * Common helper function to test RMA operation with injected failure
     */
    void test_rma_with_injected_failure(failure_side_t failure_side, unsigned op_mask) {
        const size_t size        = rma_msg_size();
        const std::string op_str = op_name(op_mask);

        /* TODO: cover case when wireup is in progress, flush here is to complete wireup */
        flush_workers();

        std::vector<ucp_lane_index_t> rma_bw_lanes = get_lanes(op_mask);

        mem_buffer lbuf(size, UCS_MEMORY_TYPE_HOST);
        mapped_buffer rbuf(size, receiver());
        ucs::handle<ucp_rkey_h> rkey = rbuf.rkey(sender());

        if (op_mask & TEST_OP_PUT) {
            lbuf.pattern_fill(m_seed);
        } else {
            ASSERT_TRUE(op_mask & TEST_OP_GET);
            rbuf.pattern_fill(m_seed);
        }

        UCS_TEST_MESSAGE << "Attempting " << op_str << " operation before failure injection...";
        ucs_status_t status = do_rma_and_wait(sender().ep(0, INJECTED_EP_INDEX), op_mask,
                                              lbuf, rbuf, rkey.get(), size);
        EXPECT_EQ(UCS_OK, status) << op_str << " operation returned status: "
                                  << ucs_status_string(status);

        ucp_ep_h ucp_ep_for_injection = get_ucp_ep_for_err_injection(failure_side);
        for (size_t lane_idx = 0; lane_idx < rma_bw_lanes.size() - 1; ++lane_idx) {
            ucp_lane_index_t lane = rma_bw_lanes[lane_idx];
            uct_ep_h uct_ep_for_injection = ucp_ep_get_lane(ucp_ep_for_injection, lane);
            status = uct_ep_invalidate(uct_ep_for_injection, 0);
            if (status == UCS_ERR_UNSUPPORTED) {
                UCS_TEST_SKIP_R("uct_ep_invalidate is not supported");
            }

            EXPECT_EQ(UCS_OK, status) << "uct_ep_invalidate returned status: "
                                    << ucs_status_string(status);

            UCS_TEST_MESSAGE << "Attempting " << op_str
                             << " operation after failure injection on lane "
                             << size_t(lane) << '/' << rma_bw_lanes.size() << "...";
            status = do_rma_and_wait(sender().ep(0, INJECTED_EP_INDEX), op_mask, lbuf, rbuf,
                                     rkey.get(), size);
            EXPECT_EQ(UCS_OK, status) << op_str << " operation returned status: "
                                    << ucs_status_string(status);

            for (ucp_lane_index_t valid_lane = lane_idx + 1; valid_lane < rma_bw_lanes.size();
                 ++valid_lane) {
                const ucp_ep_config_t *ep_config = ucp_ep_config(sender().ep(0, INJECTED_EP_INDEX));
                ASSERT_FALSE(UCS_BIT(UCP_LANE_TYPE_FAILED) &
                             ep_config->key.lanes[rma_bw_lanes[valid_lane]].lane_types)
                    << "Lane " << size_t(rma_bw_lanes[valid_lane]) << " has being marked as failed after "
                    << "failure injection on lane " << size_t(lane);
            }
        }

        short_progress_loop();
        ASSERT_EQ(0, m_total_err_count) << "Error callback invoked " << m_total_err_count << " times";
        UCS_TEST_MESSAGE << "Success";
    }

    void test_recovery(unsigned op_mask) {
        if (op_mask & TEST_OP_ALL_LANES_FAILED) {
            // Recovery is not expected, it depends on timings
            return;
        }

        UCS_TEST_MESSAGE << "Checking recovery status...";

        wait_for_cond([this]() {
            return ucp_ep_get_failed_lanes(sender().ep(0, INJECTED_EP_INDEX)) == 0;
        }, [this]() {
            short_progress_loop();
        });

        const ucp_lane_map_t failed_lanes =
                ucp_ep_get_failed_lanes(sender().ep(0, INJECTED_EP_INDEX));
        ASSERT_EQ(0, failed_lanes)
            << "Failed lanes are not recovered" << std::hex << failed_lanes;
        for (ucp_lane_index_t lane_idx = 0;
             lane_idx < ucp_ep_num_lanes(sender().ep(0, INJECTED_EP_INDEX));) {
            if (ucp_wireup_ep_test(ucp_ep_get_lane(sender().ep(0, INJECTED_EP_INDEX), lane_idx))) {
                short_progress_loop();
                continue;
            }

            ++lane_idx;
        }

        if (op_mask & TEST_OP_AM) {
            ucs_status_t status = do_am_send_and_wait(sender().ep(0, INJECTED_EP_INDEX),
                                                      am_msg_size(), true);
            EXPECT_EQ(UCS_OK, status) << "AM operation returned status: "
                                      << ucs_status_string(status);
        }

        if (op_mask & TEST_OP_PUT) {
            mem_buffer lbuf(rma_msg_size(), UCS_MEMORY_TYPE_HOST);
            mapped_buffer rbuf(rma_msg_size(), receiver());
            ucs::handle<ucp_rkey_h> rkey = rbuf.rkey(sender());
            lbuf.pattern_fill(m_seed);
            ucs_status_t status = do_put_and_wait(sender().ep(0, INJECTED_EP_INDEX), lbuf, rbuf,
                                                  rkey, rma_msg_size(), true);
            EXPECT_EQ(UCS_OK, status) << "PUT operation returned status: "
                                      << ucs_status_string(status);
        }

        if (op_mask & TEST_OP_GET) {
            mem_buffer lbuf(rma_msg_size(), UCS_MEMORY_TYPE_HOST);
            mapped_buffer rbuf(rma_msg_size(), receiver());
            ucs::handle<ucp_rkey_h> rkey = rbuf.rkey(sender());
            rbuf.pattern_fill(m_seed);
            ucs_status_t status = do_get_and_wait(sender().ep(0, INJECTED_EP_INDEX), lbuf, rbuf,
                                                  rkey, rma_msg_size(), true);
            EXPECT_EQ(UCS_OK, status) << "GET operation returned status: "
                                      << ucs_status_string(status);
        }

        ASSERT_EQ(0, m_total_err_count) << "Error callback invoked " << m_total_err_count
                                        << " times";
        UCS_TEST_MESSAGE << "All lanes are operational";
    }

    void do_test(failure_side_t failure_side) {
        const unsigned op_mask = get_variant_value();

        if (ucs_test_all_flags(op_mask, TEST_OP_PUT | TEST_OP_AM | TEST_OP_FLUSH)) {
            test_put_am_flush_with_injected_failure(failure_side, op_mask);
        } else if (op_mask & TEST_OP_AM) {
            ASSERT_FALSE(op_mask & (TEST_OP_PUT|TEST_OP_GET));
            test_am_with_injected_failure(failure_side, op_mask);
        } else {
            ASSERT_TRUE(op_mask & (TEST_OP_PUT|TEST_OP_GET));
            test_rma_with_injected_failure(failure_side, op_mask);
        }

        test_recovery(op_mask);
    }
protected:
    size_t msg_size_by_variant(size_t default_size) const {
        const unsigned op_mask = get_variant_value();

        if (op_mask & TEST_MSG_SIZE_SMALL) {
            return SMALL_MSG_SIZE;
        }

        if (op_mask & TEST_MSG_SIZE_MEDIUM) {
            return ucs::limit_buffer_size(MEDIUM_MSG_SIZE);
        }

        if (op_mask & TEST_MSG_SIZE_LARGE) {
            return ucs::limit_buffer_size(LARGE_MSG_SIZE /
                                          ucs::test_time_multiplier());
        }

        return ucs::limit_buffer_size(default_size);
    }

    size_t rma_msg_size() const {
        return msg_size_by_variant(LARGE_MSG_SIZE /
                                   ucs::test_time_multiplier());
    }

    size_t am_msg_size() const {
        return msg_size_by_variant(UCS_KBYTE);
    }

    static std::string op_name(unsigned op_mask)
    {
        std::string name;

        if (op_mask & TEST_OP_PUT) {
            name += "PUT|";
        }

        if (op_mask & TEST_OP_GET) {
            name += "GET|";
        }

        if (op_mask & TEST_OP_AM) {
            name += "AM|";
        }

        if (op_mask & TEST_OP_FLUSH) {
            name += "FLUSH|";
        }

        if (op_mask & TEST_OP_ALL_LANES_FAILED) {
            name += "ALL_LANES_FAILED|";
        }

        if (op_mask & TEST_MSG_SIZE_SMALL) {
            name += "MSG_SMALL|";
        }

        if (op_mask & TEST_MSG_SIZE_MEDIUM) {
            name += "MSG_MEDIUM|";
        }

        if (op_mask & TEST_MSG_SIZE_LARGE) {
            name += "MSG_LARGE|";
        }

        if (!name.empty()) {
            name.pop_back();
        }

        return name;
    }

    ucs_status_t do_am_send_and_wait(ucp_ep_h ep, size_t size, bool flush_after) {
        m_am_received = false;
        /* A short or bcopy send completes when posted: check this AM arrived,
         * not the data of the previous one */
        m_am_rbuf.clear();

        mem_buffer sbuf(size, UCS_MEMORY_TYPE_HOST);
        sbuf.pattern_fill(m_seed, size);

        ucp_request_param_t param = {};

        ucs_status_ptr_t sptr = ucp_am_send_nbx(ep, AM_ID, NULL, 0, sbuf.ptr(),
                                                size, &param);
        if (flush_after) {
            ucs_status_t status = request_wait(ucp_ep_flush_nbx(ep, &param));
            if (status != UCS_OK) {
                request_wait(sptr);
                return status;
            }
        }

        ucs_status_t status = request_wait(sptr);
        if (status != UCS_OK) {
            return status;
        }

        wait_for_value(&m_am_received, true);
        if (!m_am_received) {
            return UCS_ERR_TIMED_OUT;
        }

        EXPECT_EQ(size, m_am_rbuf.size());
        mem_buffer::pattern_check(m_am_rbuf.data(), size, m_seed);
        return UCS_OK;
    }

    ucs_status_t do_put_and_wait(ucp_ep_h ep, mem_buffer &lbuf, mapped_buffer &rbuf,
                                 ucp_rkey_h rkey, size_t size, bool flush) {
        rbuf.memset(0);
        ucs_status_ptr_t put_status_ptr   = ucp_put_nbx(ep, lbuf.ptr(), size, uintptr_t(rbuf.ptr()),
                                                        rkey, &m_req_empty_param);
        ucs_status_ptr_t flush_status_ptr = flush ? ucp_ep_flush_nbx(ep, &m_req_empty_param) : NULL;
        ucs_status_t status               = request_wait(put_status_ptr);
        if (status == UCS_OK) {
            rbuf.pattern_check(m_seed, size);
        }

        EXPECT_EQ(UCS_OK, status) << "put operation returned status: " << ucs_status_string(status);
        if (flush) {
            status = request_wait(flush_status_ptr);
            EXPECT_EQ(UCS_OK, status) << "flush operation returned status: " << ucs_status_string(status);
        }

        return status;
    }

    ucs_status_t do_get_and_wait(ucp_ep_h ep, mem_buffer &lbuf, mapped_buffer &rbuf,
                                 ucp_rkey_h rkey, size_t size, bool flush) {
        ucp_request_param_t param = {};

        lbuf.memset(0);
        ucs_status_ptr_t status_ptr       = ucp_get_nbx(ep, lbuf.ptr(), size, uintptr_t(rbuf.ptr()), rkey, &param);
        ucs_status_ptr_t flush_status_ptr = flush ? ucp_ep_flush_nbx(ep, &param) : NULL;
        ucs_status_t status               = request_wait(status_ptr);
        EXPECT_EQ(UCS_OK, status) << "get operation returned status: " << ucs_status_string(status);
        if (status == UCS_OK) {
            lbuf.pattern_check(m_seed, size);
        }

        if (flush) {
            status = request_wait(flush_status_ptr);
            EXPECT_EQ(UCS_OK, status) << "flush operation returned status: " << ucs_status_string(status);
        }

        return status;
    }

    ucs_status_t do_rma_and_wait(ucp_ep_h ep, unsigned op_mask, mem_buffer &lbuf, mapped_buffer &rbuf,
                                 ucp_rkey_h rkey, size_t size) {
        if (op_mask & TEST_OP_PUT) {
            return do_put_and_wait(ep, lbuf, rbuf, rkey, size, op_mask & TEST_OP_FLUSH);
        }

        if (op_mask & TEST_OP_GET) {
            return do_get_and_wait(ep, lbuf, rbuf, rkey, size, op_mask & TEST_OP_FLUSH);
        }

        return UCS_ERR_INVALID_PARAM;
    }

protected:
    static constexpr uint64_t m_seed = 0x12345678;

    void skip_unless_rc_probe_gate() {
        /* Size bits do not change which operation the variant runs. */
        if ((get_variant_value() & ~TEST_MSG_SIZE_MASK) != TEST_OP_AM) {
            UCS_TEST_SKIP_R("pure AM variant only");
        }

        if (!has_any_transport({"rc_x", "rc_v", "rc_mlx5", "rc_verbs", "ib"})) {
            UCS_TEST_SKIP_R("probe gate applies to RC p2p lanes only");
        }
    }

    static ucs_status_t recovery_probe_fail(uct_ep_h ep, unsigned flags,
                                            uct_completion_t *comp)
    {
        return UCS_ERR_ENDPOINT_TIMEOUT;
    }

    static ucs_status_t recovery_probe_count(uct_ep_h ep, unsigned flags,
                                             uct_completion_t *comp)
    {
        /* Keepalive passes no completion, recovery probes always do */
        if (comp != NULL) {
            ++m_probe_count;
        }

        return m_probe_mock->orig_func(&ep->iface->ops.ep_check, ep, flags,
                                       comp);
    }

    static ucs_status_t recovery_probe_hold(uct_ep_h ep, unsigned flags,
                                            uct_completion_t *comp)
    {
        if ((m_held_probe_comp == NULL) && (comp != NULL)) {
            m_held_probe_comp = comp;
            return UCS_INPROGRESS;
        }

        return UCS_ERR_ENDPOINT_TIMEOUT;
    }

    static void mock_recovery_probe(ucp_worker_h worker, ucs::mock &mock,
                                    uct_ep_check_func_t ep_check)
    {
        ucp_context_h context = worker->context;
        ucp_rsc_index_t rsc_index;

        m_probe_mock = &mock;

        for (rsc_index = 0; rsc_index < context->num_tls; ++rsc_index) {
            if (!UCS_STATIC_BITMAP_GET(context->tl_bitmap, rsc_index)) {
                continue;
            }

            ucp_worker_iface_t *wiface = ucp_worker_iface(worker, rsc_index);
            if ((wiface == NULL) || (wiface->iface == NULL)) {
                continue;
            }

            uint64_t flags = wiface->attr.cap.flags;
            if ((flags & UCT_IFACE_FLAG_EP_CHECK) &&
                (flags & UCT_IFACE_FLAG_CONNECT_TO_IFACE)) {
                mock.setup(&wiface->iface->ops.ep_check, ep_check);
            }
        }
    }

    static uct_completion_t *held_probe_take()
    {
        uct_completion_t *comp = m_held_probe_comp;
        m_held_probe_comp      = NULL;
        if (comp == NULL) {
            UCS_TEST_ABORT("hold mock did not capture the in-flight probe completion");
        }

        return comp;
    }

    static void mock_invoke_completion(ucs_status_t status)
    {
        uct_invoke_completion(held_probe_take(), status);
    }

    /* Complete a probe of the sender the way the transport does, under the
     * worker async lock */
    void sender_probe_complete(uct_completion_t *comp, ucs_status_t status)
    {
        ucp_worker_h worker = sender().worker();

        UCP_WORKER_THREAD_CS_ENTER_CONDITIONAL(worker);
        UCS_ASYNC_BLOCK(&worker->async);
        uct_invoke_completion(comp, status);
        UCS_ASYNC_UNBLOCK(&worker->async);
        UCP_WORKER_THREAD_CS_EXIT_CONDITIONAL(worker);
    }

    /* Probes still held when the test ends fail, before the teardown */
    struct held_probe_release {
        /* A held probe the test took from the mock */
        uct_completion_t *taken_comp = NULL;

        ~held_probe_release()
        {
            if (taken_comp != NULL) {
                uct_invoke_completion(taken_comp, UCS_ERR_CANCELED);
            }

            if (m_held_probe_comp != NULL) {
                mock_invoke_completion(UCS_ERR_CANCELED);
            }
        }
    };

    bool wait_for_recovery_probe_in_flight(ucp_ep_h ep, ucs_time_t deadline)
    {
        ucp_ep_recovery_arg_t *arg;
        ucp_lane_index_t lane;

        while (ucs_get_time() < deadline) {
            short_progress_loop();

            arg = ucp_ep_recovery_arg(ep);
            if (arg == NULL) {
                continue;
            }

            for (lane = 0; lane < ucp_ep_num_lanes(ep); ++lane) {
                if (arg->probe[lane].comp.count != 0) {
                    return true;
                }
            }
        }

        return false;
    }

    const ucp_request_param_t m_req_empty_param = { 0 };
    std::vector<uint8_t> m_am_rbuf              = std::vector<uint8_t>(am_msg_size());
    volatile bool m_am_received                 = false;

    size_t total_err_count() const {
        return m_total_err_count;
    }

    static uct_completion_t *m_held_probe_comp;
    static ucs::mock        *m_probe_mock;
    static unsigned          m_probe_count;

    static ucs_status_t recovery_probe_hold_once(uct_ep_h ep, unsigned flags,
                                                 uct_completion_t *comp)
    {
        if ((comp != NULL) && (m_probe_count++ == 0)) {
            m_held_probe_comp = comp;
            return UCS_INPROGRESS;
        }

        return m_probe_mock->orig_func(&ep->iface->ops.ep_check, ep, flags,
                                       comp);
    }

    void progress_for(double sec)
    {
        ucs_time_t deadline = ucs_get_time() + ucs_time_from_sec(sec);

        while (ucs_get_time() < deadline) {
            short_progress_loop();
        }
    }

    ucp_lane_index_t setup_one_failed_lane(ucs::mock &mock);
    void run_probe_completes_after_handshake();
    void run_probe_fail_after_peer_recovered();

#if HAVE_IB && UCT_UD_EP_DEBUG_HOOKS
    static ucs_status_t drop_ud_rx(uct_ud_iface_t *iface, uct_ud_neth_t *neth)
    {
        return UCS_ERR_BUSY;
    }

    /* The UD interface of entity @a e on the device of @a lane of @a ep */
    static uct_ud_iface_t *
    lane_ud_iface(const entity &e, ucp_ep_h ep, ucp_lane_index_t lane)
    {
        ucp_context_h context    = e.ucph();
        ucp_rsc_index_t lane_rsc = ucp_ep_get_rsc_index(ep, lane);
        ucp_worker_iface_t *wiface;
        ucp_rsc_index_t rsc_index;

        for (rsc_index = 0; rsc_index < context->num_tls; ++rsc_index) {
            if (!UCS_STATIC_BITMAP_GET(context->tl_bitmap, rsc_index) ||
                (context->tl_rscs[rsc_index].dev_index !=
                 context->tl_rscs[lane_rsc].dev_index)) {
                continue;
            }

            wiface = ucp_worker_iface(e.worker(), rsc_index);
            if ((wiface != NULL) && (wiface->iface != NULL) &&
                (wiface->iface->ops.iface_flush == uct_ud_iface_flush)) {
                return ucs_derived_of(wiface->iface, uct_ud_iface_t);
            }
        }

        return NULL;
    }
#endif

    void run_initiator_failure();
    void run_target_failure();
    void run_probe_gated_recovery();
    void run_teardown_with_outstanding_probe();
    void run_worker_flush_during_recovery();
    void run_recovery_retries_exhausted_live_lanes();
    void run_recovery_one_way_link();
    void run_recovery_probe_after_peer_connected();
    void run_recovery_ends_with_probe_in_flight();
    void run_recovery_gives_up_with_probe_in_flight();

    /* A lane of @a lanes on which an endpoint sends nothing by itself: no AM,
     * wireup or keepalive role on either endpoint */
    ucp_lane_index_t
    lane_without_own_traffic(const std::vector<ucp_lane_index_t> &lanes,
                             ucp_ep_h ep1, ucp_ep_h ep2) const
    {
        for (ucp_lane_index_t lane : lanes) {
            bool busy = false;

            for (ucp_ep_h ep : {ep1, ep2}) {
                const ucp_ep_config_key_t *key = &ucp_ep_config(ep)->key;

                busy = busy || (lane == key->am_lane) ||
                       (lane == key->wireup_msg_lane) ||
                       (lane == key->keepalive_lane);
            }

            if (!busy) {
                return lane;
            }
        }

        return UCP_NULL_LANE;
    }

private:
    size_t m_initiator_err_count = 0;
    size_t m_total_err_count     = 0;
    ucs_status_t m_err_status    = UCS_OK;
};

uct_completion_t *test_ucp_fault_tolerance::m_held_probe_comp = NULL;
ucs::mock *test_ucp_fault_tolerance::m_probe_mock             = NULL;
unsigned test_ucp_fault_tolerance::m_probe_count              = 0;

/* Same cases as test_ucp_fault_tolerance with UCX_FAILOVER_MODE=token, but
 * only on transports that support token based retransmits. */
class test_ucp_fault_tolerance_tf : public test_ucp_fault_tolerance {
public:
    test_ucp_fault_tolerance_tf() {
        modify_config("FAILOVER_MODE", "token");
        /* IB MD key; set UCX_IB_INDIRECT_ATOMIC so token mode stays selected. */
        modify_config("IB_INDIRECT_ATOMIC", "n", SETENV_IF_NOT_EXIST);
    }
};

class test_ucp_flush_lane_recovery : public test_ucp_fault_tolerance {
public:
    static void get_test_variants(std::vector<ucp_test_variant> &variants)
    {
        add_variant_with_value(variants, UCP_FEATURE_AM, TEST_OP_AM, "am");
    }

    static ucs::mock *m_flush_mock;
    static uct_completion_t *m_held_flush_comp;
    static unsigned m_held_flush_count;
    static uct_ep_h m_replacement_uct_ep;
    static unsigned m_replacement_flush_count;

    static ucs_status_t hold_flush(uct_ep_h ep, unsigned flags,
                                   uct_completion_t *comp)
    {
        ucs_assert(m_flush_mock != NULL);

        if (comp == NULL) {
            return m_flush_mock->orig_func(&ep->iface->ops.ep_flush, ep, flags,
                                           comp);
        }

        if (m_held_flush_comp == NULL) {
            m_held_flush_comp = comp;
        }

        if (comp == m_held_flush_comp) {
            if (ep == m_replacement_uct_ep) {
                ++m_replacement_flush_count;
            }

            ++m_held_flush_count;
            return UCS_INPROGRESS;
        }

        return m_flush_mock->orig_func(&ep->iface->ops.ep_flush, ep, flags,
                                       comp);
    }

    static void drain_held_flush(ucp_ep_h ep, ucp_request_t *req)
    {
        unsigned i;

        if ((req == NULL) || (m_held_flush_comp == NULL)) {
            return;
        }

        UCP_WORKER_THREAD_CS_ENTER_CONDITIONAL(ep->worker);
        for (i = 0; (i < 100) &&
                    (req->send.state.uct_comp.count > 0); ++i) {
            uct_invoke_completion(m_held_flush_comp, UCS_OK);
        }
        UCP_WORKER_THREAD_CS_EXIT_CONDITIONAL(ep->worker);
    }

    class held_flush_request_guard {
    public:
        held_flush_request_guard(test_ucp_flush_lane_recovery &test,
                                 ucp_ep_h ep, ucs_status_ptr_t &request,
                                 ucp_request_t *&req)
            : m_test(test), m_ep(ep), m_request(request), m_req(req)
        {
        }

        ~held_flush_request_guard()
        {
            cleanup_flush();
        }

        void cleanup()
        {
            ucs_status_ptr_t request;

            cleanup_flush();
            if (m_req != NULL) {
                EXPECT_EQ(0, m_req->send.state.uct_comp.count);
            }

            request   = m_request;
            m_request = NULL;
            m_req     = NULL;
            if (UCS_PTR_IS_PTR(request)) {
                EXPECT_UCS_OK(m_test.request_wait(request));
            }
        }

    private:
        void cleanup_flush()
        {
            m_flush_mock = NULL;
            drain_held_flush(m_ep, m_req);
        }

        test_ucp_flush_lane_recovery &m_test;
        ucp_ep_h m_ep;
        ucs_status_ptr_t &m_request;
        ucp_request_t *&m_req;
    };

};

ucs::mock *test_ucp_flush_lane_recovery::m_flush_mock             = NULL;
uct_completion_t *test_ucp_flush_lane_recovery::m_held_flush_comp = NULL;
unsigned test_ucp_flush_lane_recovery::m_held_flush_count         = 0;
uct_ep_h test_ucp_flush_lane_recovery::m_replacement_uct_ep       = NULL;
unsigned test_ucp_flush_lane_recovery::m_replacement_flush_count  = 0;

UCS_TEST_P(test_ucp_flush_lane_recovery, lane_replaced_during_inprogress_flush,
           "MAX_EAGER_LANES=8", "RECOVERY_RETRIES=100")
{
    ucp_request_param_t param      = {};
    ucp_request_t *req             = NULL;
    ucs_status_ptr_t request       = NULL;
    ucs_status_t invalidate_status = UCS_OK;
    bool invalidate_unsupported    = false;
    std::vector<ucp_lane_index_t> lanes;
    ucp_ep_h ep;
    uct_ep_h original_uct_ep;
    ucp_lane_index_t lane;
    uint32_t lane_generation;
    unsigned initial_flush_count;
    unsigned i;

    flush_workers();
    lanes                     = get_lanes(TEST_OP_AM);
    ep                        = sender().ep(0, INJECTED_EP_INDEX);
    lane                      = lanes[0];
    original_uct_ep           = ucp_ep_get_lane(ep, lane);
    m_held_flush_comp         = NULL;
    m_held_flush_count        = 0;
    m_replacement_uct_ep      = NULL;
    m_replacement_flush_count = 0;
    lane_generation           = ep->ext->lane_generation;

    {
        held_flush_request_guard guard(*this, ep, request, req);
        {
            ucs::mock mock;
            m_flush_mock = &mock;
            mock_ep_flush(ep, mock, hold_flush);

            request = ucp_ep_flush_nbx(ep, &param);
            ASSERT_TRUE(UCS_PTR_IS_PTR(request));
            req = static_cast<ucp_request_t*>(request) - 1;
            ASSERT_TRUE(m_held_flush_comp != NULL);
            ASSERT_EQ(req->send.state.uct_comp.func, m_held_flush_comp->func);

            invalidate_status = uct_ep_invalidate(original_uct_ep, 0);
            if (invalidate_status != UCS_ERR_UNSUPPORTED) {
                ASSERT_UCS_OK(invalidate_status);
                ASSERT_UCS_OK(do_am_send_and_wait(ep, am_msg_size(), false));

                wait_for_cond([ep, lane, original_uct_ep]() {
                    uct_ep_h uct_ep = ucp_ep_get_lane(ep, lane);

                    return (uct_ep != original_uct_ep) &&
                           !ucp_wireup_ep_test(uct_ep) &&
                           (ucp_ep_get_failed_lanes(ep) == 0);
                }, [this]() {
                    short_progress_loop();
                });

                m_replacement_uct_ep = ucp_ep_get_lane(ep, lane);
                mock_ep_flush(ep, mock, hold_flush);
                ASSERT_NE(lane_generation, ep->ext->lane_generation);
                initial_flush_count = m_held_flush_count;

                UCP_WORKER_THREAD_CS_ENTER_CONDITIONAL(ep->worker);
                for (i = 0; (i < initial_flush_count) &&
                            (req->send.state.uct_comp.count > 0); ++i) {
                    uct_invoke_completion(m_held_flush_comp, UCS_OK);
                }
                UCP_WORKER_THREAD_CS_EXIT_CONDITIONAL(ep->worker);

                EXPECT_GT(m_held_flush_count, initial_flush_count);
                EXPECT_GT(m_replacement_flush_count, 0u);
            } else {
                invalidate_unsupported = true;
            }
        }

        guard.cleanup();
    }

    if (invalidate_unsupported) {
        UCS_TEST_SKIP_R("uct_ep_invalidate is not supported");
    }

    test_recovery(TEST_OP_AM);
}

UCP_INSTANTIATE_TEST_CASE_TLS(test_ucp_flush_lane_recovery, shm_ib, "shm,ib")

void test_ucp_fault_tolerance::run_initiator_failure()
{
    if (has_any_transport({"ud_v", "ud_x"}) &&
        (get_variant_value() & (TEST_OP_AM | TEST_OP_ALL_LANES_FAILED))) {
        UCS_TEST_SKIP_R("UD transport BUG: local AM or all-lanes invalidation "
                        "leads to assertion failure in ud_ep_purge");
    }

    do_test(FAILURE_SIDE_INITIATOR);
}

void test_ucp_fault_tolerance::run_target_failure()
{
    do_test(FAILURE_SIDE_TARGET);
}

void test_ucp_fault_tolerance::run_probe_gated_recovery()
{
    skip_unless_rc_probe_gate();

    ucs::mock mock;
    mock_recovery_probe(sender().worker(), mock, recovery_probe_count);

    test_am_with_injected_failure(FAILURE_SIDE_TARGET, TEST_OP_AM);

    ucp_ep_h ep = sender().ep(0, INJECTED_EP_INDEX);

    /* Target invalidate shows up on the sender later. Lanes that are not
     * failed yet are not a finished recovery, so wait until the aux probe
     * has been armed and the failed lanes are clear. */
    wait_for_cond([ep]() {
        return (m_probe_count != 0) && (ucp_ep_get_failed_lanes(ep) == 0);
    }, [this]() {
        short_progress_loop();
    });

    ASSERT_EQ(0, ucp_ep_get_failed_lanes(ep))
            << "failed lanes are not recovered";
    EXPECT_NE(0u, m_probe_count)
            << "RC p2p lane recovery completed without arming an aux probe";
}

void test_ucp_fault_tolerance::run_teardown_with_outstanding_probe()
{
    skip_unless_rc_probe_gate();

    ucs::mock mock;
    mock_recovery_probe(sender().worker(), mock, recovery_probe_hold);

    test_am_with_injected_failure(FAILURE_SIDE_INITIATOR, TEST_OP_AM);

    ucp_ep_h ep = sender().ep(0, INJECTED_EP_INDEX);
    if (ucp_ep_get_failed_lanes(ep) == 0) {
        UCS_TEST_SKIP_R("no RC p2p lane was marked failed");
    }

    ASSERT_TRUE(wait_for_recovery_probe_in_flight(
            ep, ucs_get_time() + ucs_time_from_sec(5.0)))
            << "could not catch an aux probe in flight to exercise teardown";

    void *creq = sender().disconnect_nb(0, INJECTED_EP_INDEX,
                                        UCP_EP_CLOSE_FLAG_FORCE);
    mock_invoke_completion(UCS_ERR_CANCELED);
    ASSERT_FALSE(UCS_PTR_IS_ERR(creq))
            << "disconnect failed: "
            << ucs_status_string(UCS_PTR_STATUS(creq));
    if (UCS_PTR_IS_PTR(creq)) {
        EXPECT_EQ(UCS_OK, request_wait(creq));
    }
}

void test_ucp_fault_tolerance::run_worker_flush_during_recovery()
{
    skip_unless_rc_probe_gate();

    m_held_probe_comp = NULL;

    ucs::mock mock;
    mock_recovery_probe(sender().worker(), mock, recovery_probe_hold);

    test_am_with_injected_failure(FAILURE_SIDE_INITIATOR, TEST_OP_AM);

    ucp_ep_h ep = sender().ep(0, INJECTED_EP_INDEX);
    if (ucp_ep_get_failed_lanes(ep) == 0) {
        UCS_TEST_SKIP_R("no RC p2p lane was marked failed");
    }

    ASSERT_TRUE(wait_for_recovery_probe_in_flight(ep, ucs::get_deadline(5.0)))
            << "could not catch an aux recovery probe in flight";

    void *flush_req = sender().flush_worker_nb(0);
    ASSERT_FALSE(UCS_PTR_IS_ERR(flush_req))
            << "worker flush failed: "
            << ucs_status_string(UCS_PTR_STATUS(flush_req));

    ucs_status_t flush_status = UCS_PTR_STATUS(flush_req);
    ucs_time_t deadline       = ucs::get_deadline(1.0);
    while (UCS_PTR_IS_PTR(flush_req) && (flush_status == UCS_INPROGRESS) &&
           (ucs_get_time() < deadline)) {
        short_progress_loop();
        flush_status = ucp_request_check_status(flush_req);
    }

    /*
     * Close the endpoint and release the held probe before checking the
     * result, so the test can clean up even when the regression leaves
     * worker flush incomplete.
     */
    void *close_req = sender().disconnect_nb(0, INJECTED_EP_INDEX,
                                             UCP_EP_CLOSE_FLAG_FORCE);
    mock_invoke_completion(UCS_ERR_CANCELED);
    ASSERT_FALSE(UCS_PTR_IS_ERR(close_req))
            << "disconnect failed: "
            << ucs_status_string(UCS_PTR_STATUS(close_req));
    if (UCS_PTR_IS_PTR(close_req)) {
        EXPECT_EQ(UCS_OK, request_wait(close_req));
    }

    EXPECT_EQ(UCS_OK, flush_status)
            << "idle recovery wireup endpoint blocked worker flush";
    if (UCS_PTR_IS_PTR(flush_req)) {
        EXPECT_EQ(UCS_OK, request_wait(flush_req));
    }
}

void test_ucp_fault_tolerance::run_recovery_retries_exhausted_live_lanes()
{
    skip_unless_rc_probe_gate();

    ucs::mock mock;
    mock_recovery_probe(sender().worker(), mock, recovery_probe_fail);
    test_am_with_injected_failure(FAILURE_SIDE_INITIATOR, TEST_OP_AM);

    ucp_ep_h ep = sender().ep(0, INJECTED_EP_INDEX);
    if (ucp_ep_get_failed_lanes(ep) == 0) {
        UCS_TEST_SKIP_R("no RC p2p lane was marked failed");
    }

    ASSERT_NE(nullptr, ucp_ep_recovery_arg(ep));
    wait_for_cond([ep]() { return ucp_ep_recovery_arg(ep) == NULL; },
                  [this]() { short_progress_loop(); });
    ASSERT_EQ(nullptr, ucp_ep_recovery_arg(ep))
            << "recovery retries were not exhausted";

    EXPECT_EQ(0, total_err_count())
            << "EP was failed even though live lanes remained";
    EXPECT_NE(0, ucp_ep_get_failed_lanes(ep))
            << "failed lanes were cleared without a successful probe";
    EXPECT_EQ(UCS_OK, do_am_send_and_wait(ep, am_msg_size(), true))
            << "data did not flow on live lanes after recovery give-up";
}

ucp_lane_index_t
test_ucp_fault_tolerance::setup_one_failed_lane(ucs::mock &mock)
{
    ucp_ep_h ep           = sender().ep(0, INJECTED_EP_INDEX);
    ucp_lane_index_t lane = UCP_NULL_LANE;
    ucs_status_t status;

    flush_workers();
    for (ucp_lane_index_t l : get_lanes(TEST_OP_AM)) {
        if (ucp_ep_is_lane_p2p(ep, l) &&
            (l != ucp_ep_config(ep)->key.am_lane)) {
            lane = l;
            break;
        }
    }

    if (lane == UCP_NULL_LANE) {
        return UCP_NULL_LANE;
    }

    m_held_probe_comp = NULL;
    m_probe_count     = 0;
    mock_recovery_probe(sender().worker(), mock, recovery_probe_hold_once);

    status = uct_ep_invalidate(ucp_ep_get_lane(ep, lane), 0);
    if (status == UCS_ERR_UNSUPPORTED) {
        return UCP_NULL_LANE;
    }

    EXPECT_UCS_OK(status);
    EXPECT_UCS_OK(do_am_send_and_wait(ep, am_msg_size(), true));
    UCS_TEST_MESSAGE << "failed lane " << int(lane);
    return lane;
}

void test_ucp_fault_tolerance::run_probe_completes_after_handshake()
{
    ucp_ep_h ep                = sender().ep(0, INJECTED_EP_INDEX);
    ucp_ep_h peer_ep           = receiver().ep(0, INJECTED_EP_INDEX);
    ucp_ep_recovery_arg_t *arg = NULL;
    ucp_wireup_ep_t *wireup_ep;
    uint16_t req_id, peer_req_id;
    ucp_lane_index_t lane;

    skip_unless_rc_probe_gate();
    ucs::mock mock;
    lane = setup_one_failed_lane(mock);
    if (lane == UCP_NULL_LANE) {
        UCS_TEST_SKIP_R("no suitable p2p lane");
    }

    wait_for_cond([&]() {
        arg = ucp_ep_recovery_arg(ep);
        return (arg != NULL) && (m_held_probe_comp != NULL) &&
               (arg->state == UCP_EP_RECOVERY_STATE_PROBING) &&
               (ucp_ep_get_failed_lanes(peer_ep) == 0);
    }, [this]() { short_progress_loop(); });
    ASSERT_NE(nullptr, arg);
    ASSERT_EQ(&arg->probe[lane].comp, m_held_probe_comp);
    ASSERT_EQ(UCP_EP_RECOVERY_STATE_PROBING, arg->state);
    ASSERT_EQ(0, ucp_ep_get_failed_lanes(peer_ep));

    wireup_ep = ucp_wireup_ep(ucp_ep_get_lane(ep, lane));
    ASSERT_NE(nullptr, wireup_ep);
    EXPECT_EQ(UCS_BIT(lane), ucp_ep_get_failed_lanes(ep));
    EXPECT_FALSE(wireup_ep->flags & UCP_WIREUP_EP_FLAG_READY);
    EXPECT_FALSE(wireup_ep->flags & UCP_WIREUP_EP_FLAG_LOCAL_CONNECTED)
            << "lane was connected before its probe completed";

    req_id      = ep->ext->tf_request_id;
    peer_req_id = peer_ep->ext->tf_request_id;
    UCP_WORKER_THREAD_CS_ENTER_CONDITIONAL(ep->worker);
    mock_invoke_completion(UCS_OK);
    UCP_WORKER_THREAD_CS_EXIT_CONDITIONAL(ep->worker);

    wait_for_cond([&]() {
        return (ucp_ep_get_failed_lanes(ep) == 0) &&
               (ucp_ep_recovery_arg(ep) == NULL) &&
               !ucp_wireup_ep_test(ucp_ep_get_lane(ep, lane));
    }, [this]() { short_progress_loop(); });
    ASSERT_EQ(0, ucp_ep_get_failed_lanes(ep)) << "late probe did not recover";

    progress_for(0.5);
    ASSERT_EQ(req_id, ep->ext->tf_request_id)
            << "late probe started another LANES_ADDR exchange";
    EXPECT_EQ(peer_req_id, peer_ep->ext->tf_request_id);
    EXPECT_EQ(0, ucp_ep_get_failed_lanes(peer_ep));
    EXPECT_EQ(1u, m_probe_count);
    EXPECT_UCS_OK(do_am_send_and_wait(ep, am_msg_size(), true));
    EXPECT_EQ(0, total_err_count());
}

void test_ucp_fault_tolerance::run_probe_fail_after_peer_recovered()
{
    ucp_ep_h ep                = sender().ep(0, INJECTED_EP_INDEX);
    ucp_ep_h peer_ep           = receiver().ep(0, INJECTED_EP_INDEX);
    ucp_ep_recovery_arg_t *arg = NULL;
    mem_buffer sbuf(UCS_MBYTE, UCS_MEMORY_TYPE_HOST);
    ucp_request_param_t param  = {};
    uint16_t req_id;
    ucp_lane_index_t lane;
    int i;

    skip_unless_rc_probe_gate();
    ucs::mock mock;
    lane = setup_one_failed_lane(mock);
    if (lane == UCP_NULL_LANE) {
        UCS_TEST_SKIP_R("no suitable p2p lane");
    }

    /* Our probe is held, the peer has fully recovered its lane */
    wait_for_cond([&]() {
        arg = ucp_ep_recovery_arg(ep);
        return (arg != NULL) && (m_held_probe_comp != NULL) &&
               (ucp_ep_get_failed_lanes(peer_ep) == 0);
    }, [this]() { short_progress_loop(); });
    ASSERT_NE(nullptr, arg);
    ASSERT_EQ(&arg->probe[lane].comp, m_held_probe_comp);
    ASSERT_EQ(0, ucp_ep_get_failed_lanes(peer_ep));

    /* Our probe fails: the retry makes the peer rebuild its lane */
    req_id = ep->ext->tf_request_id;
    UCP_WORKER_THREAD_CS_ENTER_CONDITIONAL(ep->worker);
    mock_invoke_completion(UCS_ERR_ENDPOINT_TIMEOUT);
    UCP_WORKER_THREAD_CS_EXIT_CONDITIONAL(ep->worker);

    wait_for_cond([&]() {
        return (ucp_ep_get_failed_lanes(ep) == 0) &&
               (ucp_ep_get_failed_lanes(peer_ep) == 0) &&
               (ucp_ep_recovery_arg(ep) == NULL) &&
               !ucp_wireup_ep_test(ucp_ep_get_lane(ep, lane));
    }, [this]() { short_progress_loop(); });
    ASSERT_EQ(0, ucp_ep_get_failed_lanes(ep)) << "lane did not recover";
    UCS_TEST_MESSAGE << "recovered, requests sent after the failed probe: "
                     << int(uint16_t(ep->ext->tf_request_id - req_id));

    /* Large AMs are split across the eager lanes, so this lane is used */
    for (i = 0; i < 50; ++i) {
        request_wait(ucp_am_send_nbx(ep, AM_ID, NULL, 0, sbuf.ptr(),
                                     UCS_MBYTE, &param));
    }
    request_wait(ucp_ep_flush_nbx(ep, &param));
    progress_for(3.0);

    EXPECT_EQ(0, ucp_ep_get_failed_lanes(ep)) << "recovered lane failed again";
    EXPECT_EQ(uint16_t(req_id + 1), ep->ext->tf_request_id)
            << "extra recovery rounds after the lane became READY";
    EXPECT_EQ(0, total_err_count());
}

/*
 * The link of a lane works in one direction only: nothing the receiver sends
 * on the lane device reaches the sender, as when a switch or the receiver NIC
 * drops its transmit traffic. The receiver must keep the lane failed until
 * its own packets reach the sender. Here the receiver loses the UD traffic of
 * the recovery probes, its RC queue pair is not blocked.
 */
void test_ucp_fault_tolerance::run_recovery_one_way_link()
{
    skip_unless_rc_probe_gate();

#if HAVE_IB && UCT_UD_EP_DEBUG_HOOKS
    const double block_sec = 3.0;
    ucp_ep_h sender_ep, receiver_ep;
    std::vector<ucp_lane_index_t> lanes;
    uct_ud_iface_t *ud_iface;
    ucp_lane_index_t lane;
    ucs_time_t deadline;
    ucs_status_t status;
    bool recovered;

    flush_workers();
    lanes       = get_lanes(TEST_OP_AM);
    lane        = lanes[0];
    sender_ep   = sender().ep(0, INJECTED_EP_INDEX);
    receiver_ep = receiver().ep(0, INJECTED_EP_INDEX);
    if (!ucp_ep_is_lane_p2p(receiver_ep, lane)) {
        UCS_TEST_SKIP_R("the lane is not a p2p lane");
    }

    ud_iface = lane_ud_iface(sender(), sender_ep, lane);
    if (ud_iface == NULL) {
        UCS_TEST_SKIP_R("no UD transport on the lane device");
    }

    status = uct_ep_invalidate(ucp_ep_get_lane(sender_ep, lane), 0);
    if (status == UCS_ERR_UNSUPPORTED) {
        UCS_TEST_SKIP_R("uct_ep_invalidate is not supported");
    }

    ASSERT_UCS_OK(status);
    UCS_TEST_MESSAGE << "lane " << size_t(lane)
                     << ": the sender drops UD packets of the receiver";
    ud_iface->rx.hook = drop_ud_rx;
    EXPECT_UCS_OK(do_am_send_and_wait(sender_ep, am_msg_size(), false));

    wait_for_cond([receiver_ep, lane]() {
        return ucp_ep_get_failed_lanes(receiver_ep) & UCS_BIT(lane);
    }, [this]() { short_progress_loop(); });
    recovered = !(ucp_ep_get_failed_lanes(receiver_ep) & UCS_BIT(lane));
    if (!recovered) {
        deadline = ucs_get_time() + ucs_time_from_sec(block_sec);
        while (!recovered && (ucs_get_time() < deadline)) {
            short_progress_loop();
            recovered = !(ucp_ep_get_failed_lanes(receiver_ep) &
                          UCS_BIT(lane));
        }
    }

    ud_iface->rx.hook = uct_ud_iface_null_hook;
    EXPECT_FALSE(recovered)
            << "lane " << size_t(lane) << " was recovered by the receiver,"
            << " whose packets do not reach the sender";

    wait_for_cond([sender_ep, receiver_ep]() {
        return (ucp_ep_get_failed_lanes(sender_ep) == 0) &&
               (ucp_ep_get_failed_lanes(receiver_ep) == 0);
    }, [this]() { short_progress_loop(); });
    EXPECT_EQ(0, ucp_ep_get_failed_lanes(sender_ep))
            << "the sender did not recover the lane";
    EXPECT_EQ(0, ucp_ep_get_failed_lanes(receiver_ep))
            << "the receiver did not recover the lane";
    EXPECT_UCS_OK(do_am_send_and_wait(sender_ep, am_msg_size(), true));
    EXPECT_EQ(0, total_err_count());
#else
    UCS_TEST_SKIP_R("UD debug hooks are not enabled");
#endif
}

/*
 * The recovery probe of the sender completes only after the receiver has
 * connected its replacement queue pair and put the lane back into its
 * endpoint. The sender has the address of that queue pair from the address
 * exchange already, so it must connect the lane as soon as its probe
 * completes. Another address exchange would report the lane failed again,
 * and the receiver would drop its working queue pair.
 */
void test_ucp_fault_tolerance::run_recovery_probe_after_peer_connected()
{
    skip_unless_rc_probe_gate();

    ucp_ep_h sender_ep, receiver_ep;
    std::vector<ucp_lane_index_t> lanes;
    uct_ep_h receiver_uct_ep;
    ucp_lane_index_t lane;
    ucs_time_t deadline;
    ucs_status_t status;

    flush_workers();
    lanes       = get_lanes(TEST_OP_AM);
    sender_ep   = sender().ep(0, INJECTED_EP_INDEX);
    receiver_ep = receiver().ep(0, INJECTED_EP_INDEX);
    lane        = lane_without_own_traffic(lanes, sender_ep, receiver_ep);
    if (lane == UCP_NULL_LANE) {
        UCS_TEST_SKIP_R("every lane carries AM, wireup or keepalive traffic");
    }

    if (!ucp_ep_is_lane_p2p(receiver_ep, lane)) {
        UCS_TEST_SKIP_R("the lane is not a p2p lane");
    }

    ucs::mock mock;
    held_probe_release release_guard;
    mock_recovery_probe(sender().worker(), mock, recovery_probe_hold);

    status = uct_ep_invalidate(ucp_ep_get_lane(sender_ep, lane), 0);
    if (status == UCS_ERR_UNSUPPORTED) {
        UCS_TEST_SKIP_R("uct_ep_invalidate is not supported");
    }

    ASSERT_UCS_OK(status);
    UCS_TEST_MESSAGE << "lane " << size_t(lane)
                     << ": the sender holds its recovery probe";
    EXPECT_UCS_OK(do_am_send_and_wait(sender_ep, am_msg_size(), false));

    /* The receiver recovers the lane while the sender probe is in flight */
    wait_for_cond([this, receiver_ep, lane]() {
        uct_ep_h uct_ep = ucp_ep_get_lane(receiver_ep, lane);

        return (m_held_probe_comp != NULL) &&
               !(ucp_ep_get_failed_lanes(receiver_ep) & UCS_BIT(lane)) &&
               !ucp_wireup_ep_test(uct_ep) && !ucp_is_uct_ep_failed(uct_ep);
    }, [this]() { short_progress_loop(); });
    ASSERT_TRUE(m_held_probe_comp != NULL) << "no sender probe in flight";
    ASSERT_FALSE(ucp_ep_get_failed_lanes(receiver_ep) & UCS_BIT(lane))
            << "the receiver did not recover the lane";
    ASSERT_TRUE(ucp_ep_get_failed_lanes(sender_ep) & UCS_BIT(lane))
            << "the sender recovered the lane without its probe";
    receiver_uct_ep = ucp_ep_get_lane(receiver_ep, lane);

    mock_invoke_completion(UCS_OK);

    wait_for_cond([sender_ep]() {
        return ucp_ep_get_failed_lanes(sender_ep) == 0;
    }, [this]() { short_progress_loop(); });
    EXPECT_EQ(0, ucp_ep_get_failed_lanes(sender_ep))
            << "the sender did not recover the lane";

    /* A few more keepalive rounds */
    deadline = ucs_get_time() + ucs_time_from_sec(1.0);
    while (ucs_get_time() < deadline) {
        short_progress_loop();
    }

    EXPECT_EQ(receiver_uct_ep, ucp_ep_get_lane(receiver_ep, lane))
            << "the receiver replaced the queue pair of the recovered lane";
    EXPECT_EQ(0, ucp_ep_get_failed_lanes(receiver_ep))
            << "the receiver failed the recovered lane again";
    EXPECT_UCS_OK(do_am_send_and_wait(sender_ep, am_msg_size(), true));
    EXPECT_EQ(0, total_err_count());
}

/*
 * A failed lane is connected, and before the transport endpoint replaces its
 * wireup proxy, another address message of the peer names the lane: a request
 * the peer repeated, or the reply to an older request. The lane is still
 * failed, so the message probes it again. The next keepalive finds the lane
 * recovered and ends the recovery while that probe is in flight. The probe
 * completes later, when the aux endpoint of the replaced proxy is flushed or
 * destroyed.
 */
void test_ucp_fault_tolerance::run_recovery_ends_with_probe_in_flight()
{
    skip_unless_rc_probe_gate();

    struct peer_address {
        void                   *buffer  = NULL;
        ucp_unpacked_address_t unpacked = {};

        ~peer_address()
        {
            ucs_free(unpacked.address_list);
            ucs_free(buffer);
        }
    } address;
    ucp_ep_h sender_ep, receiver_ep;
    std::vector<ucp_lane_index_t> lanes;
    ucp_lane_map_t connected, probed;
    uct_completion_t *probe_comp;
    ucp_ep_recovery_arg_t *arg;
    ucp_context_h context;
    ucp_tl_bitmap_t tl_bitmap;
    ucp_lane_index_t lane;
    unsigned pack_flags;
    uint16_t req_id;
    size_t length;
    ucs_status_t status;

    flush_workers();
    lanes       = get_lanes(TEST_OP_AM);
    sender_ep   = sender().ep(0, INJECTED_EP_INDEX);
    receiver_ep = receiver().ep(0, INJECTED_EP_INDEX);
    lane        = lane_without_own_traffic(lanes, sender_ep, receiver_ep);
    if (lane == UCP_NULL_LANE) {
        UCS_TEST_SKIP_R("every lane carries AM, wireup or keepalive traffic");
    }

    if (!ucp_ep_is_lane_p2p(sender_ep, lane)) {
        UCS_TEST_SKIP_R("the lane is not a p2p lane");
    }

    ucs::mock mock;
    held_probe_release release_guard;
    mock_recovery_probe(sender().worker(), mock, recovery_probe_hold);

    status = uct_ep_invalidate(ucp_ep_get_lane(sender_ep, lane), 0);
    if (status == UCS_ERR_UNSUPPORTED) {
        UCS_TEST_SKIP_R("uct_ep_invalidate is not supported");
    }

    ASSERT_UCS_OK(status);
    EXPECT_UCS_OK(do_am_send_and_wait(sender_ep, am_msg_size(), false));

    /* The sender holds its probe, the receiver has recovered the lane */
    arg = NULL;
    wait_for_cond([&]() {
        uct_ep_h uct_ep = ucp_ep_get_lane(receiver_ep, lane);

        arg = ucp_ep_recovery_arg(sender_ep);
        return (arg != NULL) && (m_held_probe_comp != NULL) &&
               (arg->probe[lane].peer_addr != NULL) &&
               !(ucp_ep_get_failed_lanes(receiver_ep) & UCS_BIT(lane)) &&
               !ucp_wireup_ep_test(uct_ep) && !ucp_is_uct_ep_failed(uct_ep);
    }, [this]() { short_progress_loop(); });
    ASSERT_NE(nullptr, arg);
    ASSERT_EQ(&arg->probe[lane].comp, m_held_probe_comp);
    ASSERT_NE(nullptr, arg->probe[lane].peer_addr)
            << "the sender has no address of the lane";
    ASSERT_FALSE(ucp_ep_get_failed_lanes(receiver_ep) & UCS_BIT(lane))
            << "the receiver did not recover the lane";

    /* The address the receiver publishes for the lane */
    context    = receiver().ucph();
    tl_bitmap  = context->tl_bitmap;
    pack_flags = ucp_worker_default_address_pack_flags(receiver().worker()) |
                 UCP_ADDRESS_PACK_FLAG_TL_RSC_IDX;
    ASSERT_UCS_OK(ucp_address_pack(receiver().worker(), receiver_ep,
                                   &tl_bitmap, pack_flags,
                                   context->config.ext.worker_addr_version,
                                   NULL, UINT_MAX, &length, &address.buffer));
    ASSERT_UCS_OK(ucp_address_unpack(sender().worker(), address.buffer,
                                     UCP_ADDRESS_PACK_FLAGS_ALL,
                                     &address.unpacked));

    /* The probe completes, and two address messages of the receiver that name
     * the lane are handled before the transport endpoint replaces the wireup
     * proxy: the first one connects the lane, the second one probes it */
    req_id = sender_ep->ext->tf_request_id;
    UCP_WORKER_THREAD_CS_ENTER_CONDITIONAL(sender().worker());
    UCS_ASYNC_BLOCK(&sender().worker()->async);
    mock_invoke_completion(UCS_OK);
    connected  = ucp_ep_recovery_rebuild_lanes(sender_ep, UCS_BIT(lane),
                                               &address.unpacked);
    probed     = ucp_ep_recovery_rebuild_lanes(sender_ep, UCS_BIT(lane),
                                               &address.unpacked);
    probe_comp = m_held_probe_comp;
    ucp_wireup_eps_progress(sender_ep);
    UCS_ASYNC_UNBLOCK(&sender().worker()->async);
    UCP_WORKER_THREAD_CS_EXIT_CONDITIONAL(sender().worker());

    ASSERT_EQ(UCS_BIT(lane), connected)
            << "the address did not connect the lane";
    ASSERT_EQ(0, probed);
    ASSERT_EQ(&arg->probe[lane].comp, probe_comp)
            << "the connected lane was not probed again";
    ASSERT_FALSE(ucp_wireup_ep_test(ucp_ep_get_lane(sender_ep, lane)))
            << "the wireup proxy of the lane was not replaced";

    /* The keepalive ends the recovery while the probe is in flight */
    wait_for_cond([sender_ep]() {
        return (ucp_ep_get_failed_lanes(sender_ep) == 0) &&
               (ucp_ep_recovery_arg(sender_ep) == NULL);
    }, [this]() { short_progress_loop(); });
    ASSERT_EQ(0, ucp_ep_get_failed_lanes(sender_ep))
            << "the lane did not recover";
    ASSERT_EQ(nullptr, ucp_ep_recovery_arg(sender_ep))
            << "the recovery did not end";
    EXPECT_EQ(req_id, sender_ep->ext->tf_request_id);
#if UCS_ENABLE_ASSERT
    EXPECT_EQ(1u, sender_ep->refcounts.probe)
            << "the late probe does not hold the endpoint";
#endif

    /* The lane fails again before the probe completes, and the probe of the
     * new recovery is held */
    release_guard.taken_comp = held_probe_take();
    ASSERT_UCS_OK(uct_ep_invalidate(ucp_ep_get_lane(sender_ep, lane), 0));
    EXPECT_UCS_OK(do_am_send_and_wait(sender_ep, am_msg_size(), false));
    arg = NULL;
    wait_for_cond([&]() {
        arg = ucp_ep_recovery_arg(sender_ep);
        return (arg != NULL) && (m_held_probe_comp != NULL) &&
               (arg->state == UCP_EP_RECOVERY_STATE_PROBING);
    }, [this]() { short_progress_loop(); });
    ASSERT_NE(nullptr, arg) << "the lane did not fail again";
    ASSERT_EQ(&arg->probe[lane].comp, m_held_probe_comp);
    ASSERT_EQ(UCP_EP_RECOVERY_STATE_PROBING, arg->state);
    req_id = sender_ep->ext->tf_request_id;

    /* The late probe completes during the new recovery */
    sender_probe_complete(release_guard.taken_comp, UCS_OK);
    release_guard.taken_comp = NULL;
    progress_for(0.5);
    EXPECT_EQ(arg, ucp_ep_recovery_arg(sender_ep));
    EXPECT_EQ(UCP_EP_RECOVERY_STATE_PROBING, arg->state)
            << "the late probe changed the new recovery";
    EXPECT_EQ(req_id, sender_ep->ext->tf_request_id)
            << "the late probe started another address exchange";
#if UCS_ENABLE_ASSERT
    EXPECT_EQ(1u, sender_ep->refcounts.probe);
#endif

    /* The probe of the new recovery completes */
    sender_probe_complete(held_probe_take(), UCS_OK);
    wait_for_cond([sender_ep, lane]() {
        return (ucp_ep_get_failed_lanes(sender_ep) == 0) &&
               (ucp_ep_recovery_arg(sender_ep) == NULL) &&
               !ucp_wireup_ep_test(ucp_ep_get_lane(sender_ep, lane));
    }, [this]() { short_progress_loop(); });
    EXPECT_EQ(0, ucp_ep_get_failed_lanes(sender_ep))
            << "the lane did not recover again";
    EXPECT_EQ(nullptr, ucp_ep_recovery_arg(sender_ep));
#if UCS_ENABLE_ASSERT
    EXPECT_EQ(0u, sender_ep->refcounts.probe);
#endif
    EXPECT_EQ(0, ucp_ep_get_failed_lanes(receiver_ep));
    EXPECT_UCS_OK(do_am_send_and_wait(sender_ep, am_msg_size(), true));
    EXPECT_EQ(0, total_err_count());
}

/*
 * The recovery gives up on a lane after its last retry while a probe of the
 * lane is in flight. The probe completes after the recovery ended.
 */
void test_ucp_fault_tolerance::run_recovery_gives_up_with_probe_in_flight()
{
    skip_unless_rc_probe_gate();

    ucp_ep_h sender_ep, receiver_ep;
    std::vector<ucp_lane_index_t> lanes;
    ucp_ep_recovery_arg_t *arg;
    ucp_lane_index_t lane;
    uint16_t req_id;
    ucs_status_t status;

    flush_workers();
    lanes       = get_lanes(TEST_OP_AM);
    sender_ep   = sender().ep(0, INJECTED_EP_INDEX);
    receiver_ep = receiver().ep(0, INJECTED_EP_INDEX);
    lane        = lane_without_own_traffic(lanes, sender_ep, receiver_ep);
    if (lane == UCP_NULL_LANE) {
        UCS_TEST_SKIP_R("every lane carries AM, wireup or keepalive traffic");
    }

    if (!ucp_ep_is_lane_p2p(sender_ep, lane)) {
        UCS_TEST_SKIP_R("the lane is not a p2p lane");
    }

    ucs::mock mock;
    held_probe_release release_guard;
    mock_recovery_probe(sender().worker(), mock, recovery_probe_hold);

    status = uct_ep_invalidate(ucp_ep_get_lane(sender_ep, lane), 0);
    if (status == UCS_ERR_UNSUPPORTED) {
        UCS_TEST_SKIP_R("uct_ep_invalidate is not supported");
    }

    ASSERT_UCS_OK(status);
    EXPECT_UCS_OK(do_am_send_and_wait(sender_ep, am_msg_size(), false));

    arg = NULL;
    wait_for_cond([&]() {
        arg = ucp_ep_recovery_arg(sender_ep);
        return (arg != NULL) && (m_held_probe_comp != NULL);
    }, [this]() { short_progress_loop(); });
    ASSERT_NE(nullptr, arg);
    ASSERT_EQ(&arg->probe[lane].comp, m_held_probe_comp);

    /* The last retry waited for a reply in vain, while a probe that a request
     * of the peer armed is in flight: the keepalive gives up on the lane */
    UCP_WORKER_THREAD_CS_ENTER_CONDITIONAL(sender().worker());
    UCS_ASYNC_BLOCK(&sender().worker()->async);
    arg->state        = UCP_EP_RECOVERY_STATE_WAIT_REPLY;
    arg->retries_left = 1;
    ucp_ep_recovery_progress(sender_ep);
    UCS_ASYNC_UNBLOCK(&sender().worker()->async);
    UCP_WORKER_THREAD_CS_EXIT_CONDITIONAL(sender().worker());

    ASSERT_EQ(nullptr, ucp_ep_recovery_arg(sender_ep))
            << "the recovery did not give up";
    ASSERT_TRUE(ucp_ep_get_failed_lanes(sender_ep) & UCS_BIT(lane));
#if UCS_ENABLE_ASSERT
    EXPECT_EQ(1u, sender_ep->refcounts.probe)
            << "the late probe does not hold the endpoint";
#endif

    /* The probe completes after the recovery gave up */
    req_id = sender_ep->ext->tf_request_id;
    sender_probe_complete(held_probe_take(), UCS_OK);
    progress_for(1.0);
    EXPECT_EQ(nullptr, ucp_ep_recovery_arg(sender_ep))
            << "the late probe restarted the recovery";
    EXPECT_EQ(req_id, sender_ep->ext->tf_request_id)
            << "the late probe started another address exchange";
    EXPECT_TRUE(ucp_ep_get_failed_lanes(sender_ep) & UCS_BIT(lane))
            << "the late probe recovered a lane the recovery gave up on";
#if UCS_ENABLE_ASSERT
    EXPECT_EQ(0u, sender_ep->refcounts.probe);
#endif
    EXPECT_UCS_OK(do_am_send_and_wait(sender_ep, am_msg_size(), true));
    EXPECT_EQ(0, total_err_count());
}

#define UCP_FT_TEST(_fixture, _name, ...) \
    UCS_TEST_P(_fixture, _name, __VA_ARGS__) \
    { \
        run_##_name(); \
    }

#define UCP_FT_TESTS(_fixture) \
    UCP_FT_TEST(_fixture, initiator_failure, "MAX_EAGER_LANES=8", \
                "RECOVERY_RETRIES=100") \
    UCP_FT_TEST(_fixture, target_failure, "MAX_EAGER_LANES=8", \
                "RECOVERY_RETRIES=100") \
    UCP_FT_TEST(_fixture, probe_gated_recovery, "MAX_EAGER_LANES=8", \
                "RECOVERY_RETRIES=100") \
    UCP_FT_TEST(_fixture, teardown_with_outstanding_probe, \
                "MAX_EAGER_LANES=8", "RECOVERY_RETRIES=1000") \
    UCP_FT_TEST(_fixture, worker_flush_during_recovery, \
                "MAX_EAGER_LANES=8", "RECOVERY_RETRIES=1000", \
                "KEEPALIVE_INTERVAL=0.1s") \
    UCP_FT_TEST(_fixture, probe_completes_after_handshake, \
                "MAX_EAGER_LANES=8", "RECOVERY_RETRIES=100", \
                "KEEPALIVE_INTERVAL=0.1s") \
    UCP_FT_TEST(_fixture, probe_fail_after_peer_recovered, \
                "MAX_EAGER_LANES=8", "RECOVERY_RETRIES=100", \
                "KEEPALIVE_INTERVAL=0.1s") \
    UCP_FT_TEST(_fixture, recovery_retries_exhausted_live_lanes, \
                "MAX_EAGER_LANES=8", "RECOVERY_RETRIES=2", \
                "KEEPALIVE_INTERVAL=0.1s") \
    UCP_FT_TEST(_fixture, recovery_one_way_link, "MAX_EAGER_LANES=8", \
                "RECOVERY_RETRIES=1000") \
    UCP_FT_TEST(_fixture, recovery_probe_after_peer_connected, \
                "MAX_EAGER_LANES=8", "RECOVERY_RETRIES=1000") \
    UCP_FT_TEST(_fixture, recovery_ends_with_probe_in_flight, \
                "MAX_EAGER_LANES=8", "RECOVERY_RETRIES=1000") \
    UCP_FT_TEST(_fixture, recovery_gives_up_with_probe_in_flight, \
                "MAX_EAGER_LANES=8", "RECOVERY_RETRIES=1000")

UCP_FT_TESTS(test_ucp_fault_tolerance)
UCP_FT_TESTS(test_ucp_fault_tolerance_tf)

#undef UCP_FT_TEST
#undef UCP_FT_TESTS

UCP_INSTANTIATE_TEST_CASE(test_ucp_fault_tolerance)
UCP_INSTANTIATE_TEST_CASE_TLS(test_ucp_fault_tolerance_tf, rcx, "rc_x")

/* Short and single bcopy AMs complete when they are posted, so after a lane
 * failure only token replay delivers them. A peer which cannot exchange tokens
 * makes the lane fail in software, which would drop them: the eager AM of such
 * a peer has to stay on the PSN protocol, which completes after delivery. */
class test_ucp_fault_tolerance_tf_am_proto :
        public test_ucp_fault_tolerance_tf {
public:
    static void get_test_variants(std::vector<ucp_test_variant> &variants)
    {
        add_variant_with_value(variants, UCP_FEATURE_AM, TEST_OP_AM,
                               op_name(TEST_OP_AM));
    }

protected:
    std::string am_proto_name(ucp_worker_cfg_index_t cfg_index, size_t length)
    {
        ucp_worker_h worker = sender().worker();
        const ucp_proto_threshold_elem_t *thresh;
        ucp_proto_select_param_t select_param;
        ucp_proto_select_elem_t *select_elem;
        ucp_memory_info_t mem_info;

        ucp_memory_info_set_host(&mem_info);
        ucp_proto_select_param_init(&select_param, UCP_OP_ID_AM_SEND, 0, 0,
                                    UCP_DATATYPE_CONTIG, &mem_info, 1);
        select_elem = ucp_proto_select_lookup_slow(
                worker, &ucp_worker_ep_config(worker, cfg_index)->proto_select,
                0, cfg_index, UCP_WORKER_CFG_INDEX_NULL, &select_param);
        if (select_elem == NULL) {
            return "";
        }

        thresh = ucp_proto_thresholds_search_slow(select_elem->thresholds,
                                                  length);
        return (thresh == NULL) ? "" : thresh->proto_config.proto->name;
    }
};

UCS_TEST_P(test_ucp_fault_tolerance_tf_am_proto, older_peer)
{
    ucp_ep_h ep = sender().ep(0, INJECTED_EP_INDEX);
    ucp_worker_cfg_index_t cfg_index;
    ucp_ep_config_key_t key;

    flush_workers();
    key = ucp_ep_config(ep)->key;
    ASSERT_EQ(UCP_ERR_HANDLING_MODE_FAILOVER, key.err_mode);
    ASSERT_EQ("am/egr/short", am_proto_name(ep->cfg_index, 64));

    key.dst_version = UCP_MIN_DST_VERSION_WIREUP_ADDR_TOKEN - 1;
    ASSERT_UCS_OK(ucp_worker_get_ep_config(sender().worker(), &key, 0,
                                           &cfg_index));
    EXPECT_EQ("am/egr/multi/zcopy/psn", am_proto_name(cfg_index, 64));
    EXPECT_EQ("am/egr/multi/zcopy/psn", am_proto_name(cfg_index, UCS_KBYTE));
}

UCP_INSTANTIATE_TEST_CASE_TLS(test_ucp_fault_tolerance_tf_am_proto, rcx, "rc_x")

/* Many eager AMs (short or single bcopy) are in flight on the AM lane when it
 * fails. Token failover must deliver each of them once, with its payload. */
class test_ucp_fault_tolerance_tf_am_burst :
        public test_ucp_fault_tolerance_tf {
public:
    static void get_test_variants(std::vector<ucp_test_variant> &variants)
    {
        add_variant_with_value(variants, UCP_FEATURE_AM, 256, "short");
        add_variant_with_value(variants, UCP_FEATURE_AM, 4096, "bcopy");
    }

protected:
    static constexpr uint16_t BURST_AM_ID = 1;
    static constexpr unsigned BURST_COUNT = 512;

    static uint8_t burst_byte(uint32_t seq, size_t offset)
    {
        return (uint8_t)(seq * 31 + offset);
    }

    static ucs_status_t
    burst_recv_cb(void *arg, const void *header, size_t header_length,
                  void *data, size_t length, const ucp_am_recv_param_t *param)
    {
        auto *self = reinterpret_cast<test_ucp_fault_tolerance_tf_am_burst*>(
                arg);
        const uint8_t *bytes = static_cast<const uint8_t*>(data);
        uint32_t seq;
        size_t i;

        if ((param->recv_attr & UCP_AM_RECV_ATTR_FLAG_RNDV) ||
            (length != self->m_size)) {
            ++self->m_bad;
            return UCS_OK;
        }

        memcpy(&seq, data, sizeof(seq));
        for (i = sizeof(seq); i < length; ++i) {
            if (bytes[i] != burst_byte(seq, i)) {
                ++self->m_bad;
                return UCS_OK;
            }
        }

        if (seq >= self->m_counts.size()) {
            ++self->m_bad;
            return UCS_OK;
        }

        ++self->m_counts[seq];
        ++self->m_received;
        return UCS_OK;
    }

    void send_burst(ucp_ep_h ep, uint32_t first, std::vector<void*> &reqs)
    {
        ucp_request_param_t param = {};
        uint32_t seq;
        size_t i;

        for (seq = first; seq < (first + BURST_COUNT); ++seq) {
            std::vector<uint8_t> &buf = m_bufs[seq];

            buf.resize(m_size);
            memcpy(buf.data(), &seq, sizeof(seq));
            for (i = sizeof(seq); i < m_size; ++i) {
                buf[i] = burst_byte(seq, i);
            }

            reqs.push_back(ucp_am_send_nbx(ep, BURST_AM_ID, NULL, 0,
                                           buf.data(), m_size, &param));
        }
    }

    void run_am_burst(failure_side_t failure_side)
    {
        ucp_ep_h ep = sender().ep(0, INJECTED_EP_INDEX);
        ucp_am_handler_param_t param;
        std::vector<void*> reqs;
        ucp_ep_h inject_ep;
        ucp_lane_index_t am_lane;
        unsigned missing, duplicate;
        size_t seq;

        m_size = get_variant_value();
        flush_workers();

        param.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                           UCP_AM_HANDLER_PARAM_FIELD_CB |
                           UCP_AM_HANDLER_PARAM_FIELD_ARG;
        param.id         = BURST_AM_ID;
        param.cb         = burst_recv_cb;
        param.arg        = this;
        ASSERT_UCS_OK(ucp_worker_set_am_recv_handler(receiver().worker(),
                                                     &param));

        if (ucp_ep_config(ep)->key.num_lanes < 2) {
            UCS_TEST_SKIP_R("needs 2 lanes");
        }

        m_counts.assign(2 * BURST_COUNT, 0);
        m_bufs.resize(2 * BURST_COUNT);

        inject_ep = (failure_side == FAILURE_SIDE_INITIATOR) ?
                    ep : receiver().ep(0, INJECTED_EP_INDEX);
        am_lane   = ucp_ep_config(inject_ep)->key.am_lane;
        UCS_TEST_MESSAGE << "size " << m_size << " am_lane " << int(am_lane)
                         << " of " << int(ucp_ep_num_lanes(inject_ep));

        send_burst(ep, 0, reqs);
        ASSERT_UCS_OK(uct_ep_invalidate(ucp_ep_get_lane(inject_ep, am_lane),
                                        0));
        send_burst(ep, BURST_COUNT, reqs);

        EXPECT_UCS_OK(requests_wait(reqs));
        wait_for_cond([this]() {
            return (m_received + m_bad) >= (2 * BURST_COUNT);
        }, [this]() { short_progress_loop(); });
        /* Late duplicates */
        short_progress_loop();

        missing   = 0;
        duplicate = 0;
        for (seq = 0; seq < m_counts.size(); ++seq) {
            if (m_counts[seq] == 0) {
                if (missing++ < 5) {
                    UCS_TEST_MESSAGE << "missing AM " << seq;
                }
            } else if (m_counts[seq] > 1) {
                if (duplicate++ < 5) {
                    UCS_TEST_MESSAGE << "AM " << seq << " received "
                                     << m_counts[seq] << " times";
                }
            }
        }

        EXPECT_EQ(0u, m_bad) << "AMs with a wrong length or payload";
        EXPECT_EQ(0u, missing) << "AMs not received";
        EXPECT_EQ(0u, duplicate) << "AMs received more than once";
        EXPECT_EQ(0, total_err_count());
    }

    size_t m_size     = 0;
    size_t m_received = 0;
    size_t m_bad      = 0;
    std::vector<unsigned> m_counts;
    std::vector<std::vector<uint8_t>> m_bufs;
};

UCS_TEST_P(test_ucp_fault_tolerance_tf_am_burst, initiator_failure,
           "MAX_EAGER_LANES=8", "RECOVERY_RETRIES=100")
{
    run_am_burst(FAILURE_SIDE_INITIATOR);
}

UCS_TEST_P(test_ucp_fault_tolerance_tf_am_burst, target_failure,
           "MAX_EAGER_LANES=8", "RECOVERY_RETRIES=100")
{
    run_am_burst(FAILURE_SIDE_TARGET);
}

UCP_INSTANTIATE_TEST_CASE_TLS(test_ucp_fault_tolerance_tf_am_burst, rcx,
                              "rc_x")

/* A send on a FAILOVER endpoint is canceled locally when the user closes the
 * endpoint or destroys the worker while the send is still pending or in flight.
 * The send must complete once, without a restart on the endpoint which is going
 * away. */
class test_ucp_fault_tolerance_teardown : public test_ucp_fault_tolerance {
public:
    static void get_test_variants(std::vector<ucp_test_variant> &variants)
    {
        add_variant_with_value(variants, UCP_FEATURE_AM, 256, "small");
        add_variant_with_value(variants, UCP_FEATURE_AM, UCS_MBYTE, "large");
    }

protected:
    enum teardown_t {
        TEARDOWN_WORKER_DESTROY, /* Destroy the worker, do not close the ep */
        TEARDOWN_CLOSE,          /* Close the ep gracefully, destroy the worker
                                    before the close completes */
        TEARDOWN_FORCE_CLOSE     /* Close the ep by force, then destroy the
                                    worker */
    };

    void init() override
    {
        ucp_am_handler_param_t param;

        ucp_test::init();

        /* The receiver does not connect back, so only the sender starts
         * the wireup */
        sender().connect(&receiver(), get_ep_params());

        param.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                           UCP_AM_HANDLER_PARAM_FIELD_CB |
                           UCP_AM_HANDLER_PARAM_FIELD_ARG;
        param.id         = AM_ID;
        param.cb         = am_count_cb;
        param.arg        = this;
        ASSERT_UCS_OK(ucp_worker_set_am_recv_handler(receiver().worker(),
                                                     &param));

        m_sbuf.resize(get_variant_value());
    }

    static ucs_status_t
    am_count_cb(void *arg, const void *header, size_t header_length,
                void *data, size_t length, const ucp_am_recv_param_t *param)
    {
        ++static_cast<test_ucp_fault_tolerance_teardown*>(arg)->m_am_recv_count;
        return UCS_OK;
    }

    static void am_send_cb(void *request, ucs_status_t status, void *user_data)
    {
        test_ucp_fault_tolerance_teardown *self =
                static_cast<test_ucp_fault_tolerance_teardown*>(user_data);
        ucp_request_t *req = static_cast<ucp_request_t*>(request) - 1;

        ++self->m_am_send_count;
        self->m_am_send_status = status;
        /* A restart of the AM marks it as resent */
        self->m_am_restarted   = req->send.msg_proto.am.flags &
                                 UCP_AM_HDR_FLAG_RESEND;
        ucp_request_free(request);
    }

    static bool has_wireup_lane(ucp_ep_h ep)
    {
        ucp_lane_index_t lane;

        for (lane = 0; lane < ucp_ep_num_lanes(ep); ++lane) {
            if (ucp_wireup_ep_test(ucp_ep_get_lane(ep, lane))) {
                return true;
            }
        }

        return false;
    }

    void test_teardown(bool connected, teardown_t teardown)
    {
        ucp_ep_h ep = sender().ep();
        ucp_request_param_t param;
        ucs_status_ptr_t sptr;
        ucp_request_t *req;

        if (connected) {
            /* The receiver gets an AM once the wireup is complete */
            sptr = ucp_am_send_nbx(ep, AM_ID, NULL, 0, m_sbuf.data(),
                                   m_sbuf.size(), &m_req_empty_param);
            ASSERT_UCS_OK(request_wait(sptr));
            wait_for_value(&m_am_recv_count, 1u);
            ASSERT_EQ(1u, m_am_recv_count);
            wait_for_cond([ep]() { return !has_wireup_lane(ep); },
                          [this]() { short_progress_loop(); });
            ASSERT_FALSE(has_wireup_lane(ep));
        }

        param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                             UCP_OP_ATTR_FIELD_USER_DATA;
        param.cb.send      = am_send_cb;
        param.user_data    = this;
        sptr               = ucp_am_send_nbx(ep, AM_ID, NULL, 0, m_sbuf.data(),
                                             m_sbuf.size(), &param);
        ASSERT_FALSE(UCS_PTR_IS_ERR(sptr))
                << ucs_status_string(UCS_PTR_STATUS(sptr));
        if (sptr == NULL) {
            UCS_TEST_SKIP_R("the AM send completed at once");
        }

        req = static_cast<ucp_request_t*>(sptr) - 1;
        if (!connected) {
            /* The receiver does not progress, so the wireup cannot complete */
            ASSERT_TRUE(ucp_wireup_ep_test(ucp_ep_get_lane(ep, req->send.lane)))
                    << "the AM send does not wait for the wireup";
        }

        /* The worker or the close destroys the endpoint */
        sender().revoke_ep();

        if (teardown != TEARDOWN_WORKER_DESTROY) {
            /* Release the close request at once, as applications do */
            param.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS;
            param.flags        = (teardown == TEARDOWN_FORCE_CLOSE) ?
                                 UCP_EP_CLOSE_FLAG_FORCE : 0;
            sptr               = ucp_ep_close_nbx(ep, &param);
            ASSERT_FALSE(UCS_PTR_IS_ERR(sptr))
                    << ucs_status_string(UCS_PTR_STATUS(sptr));
            if (sptr != NULL) {
                ucp_request_free(sptr);
            }
        }

        if (teardown == TEARDOWN_FORCE_CLOSE) {
            /* Let the force close discard the lanes; an in-flight send may
             * complete only when the worker destroys them */
            wait_for_cond([this]() { return m_am_send_count != 0; },
                          [this]() { sender().progress(); }, 1.0);
        }

        {
            /* Both peers go away; the transports may warn about the
             * operations which they release */
            scoped_log_handler slh_warn(wrap_warns_logger);
            scoped_log_handler slh_err(wrap_errors_logger);

            sender().destroy_worker();
            receiver().destroy_worker();
        }

        EXPECT_EQ(1u, m_am_send_count)
                << "the AM send completed " << m_am_send_count << " times";
        if (connected) {
            EXPECT_TRUE((m_am_send_status == UCS_OK) ||
                        (m_am_send_status == UCS_ERR_CANCELED))
                    << ucs_status_string(m_am_send_status);
        } else {
            EXPECT_EQ(UCS_ERR_CANCELED, m_am_send_status)
                    << ucs_status_string(m_am_send_status);
        }

        EXPECT_FALSE(m_am_restarted) << "the canceled AM send was restarted";
    }

    std::vector<char> m_sbuf;
    volatile unsigned m_am_recv_count = 0;
    unsigned          m_am_send_count = 0;
    ucs_status_t      m_am_send_status = UCS_INPROGRESS;
    bool              m_am_restarted   = false;
};

UCS_TEST_P(test_ucp_fault_tolerance_teardown, pending_am_worker_destroy)
{
    test_teardown(false, TEARDOWN_WORKER_DESTROY);
}

UCS_TEST_P(test_ucp_fault_tolerance_teardown, pending_am_close)
{
    test_teardown(false, TEARDOWN_CLOSE);
}

UCS_TEST_P(test_ucp_fault_tolerance_teardown, pending_am_force_close)
{
    test_teardown(false, TEARDOWN_FORCE_CLOSE);
}

UCS_TEST_P(test_ucp_fault_tolerance_teardown, inflight_am_worker_destroy)
{
    test_teardown(true, TEARDOWN_WORKER_DESTROY);
}

UCS_TEST_P(test_ucp_fault_tolerance_teardown, inflight_am_close)
{
    test_teardown(true, TEARDOWN_CLOSE);
}

UCS_TEST_P(test_ucp_fault_tolerance_teardown, inflight_am_force_close)
{
    test_teardown(true, TEARDOWN_FORCE_CLOSE);
}

UCP_INSTANTIATE_TEST_CASE_TLS(test_ucp_fault_tolerance_teardown, tcp, "tcp")
UCP_INSTANTIATE_TEST_CASE_TLS(test_ucp_fault_tolerance_teardown, rc, "rc_v")
UCP_INSTANTIATE_TEST_CASE_TLS(test_ucp_fault_tolerance_teardown, rcx, "rc_x")
/* Token failover holds a failed lane until the peer returns its token. Then
 * the operations outstanding on the held transport endpoint are reposted, and
 * the requests from its pending queue, which waited in the recovery queue, are
 * replayed. A flush or a graceful close issued in the meantime must complete
 * after these operations. */
class test_ucp_flush_tf_held : public test_ucp_fault_tolerance_tf {
public:
    static void get_test_variants(std::vector<ucp_test_variant> &variants)
    {
        add_variant_with_value(variants, UCP_FEATURE_RMA,
                               TEST_OP_PUT | TEST_MSG_SIZE_SMALL,
                               op_name(TEST_OP_PUT | TEST_MSG_SIZE_SMALL));
        add_variant_with_value(variants, UCP_FEATURE_RMA,
                               TEST_OP_PUT | TEST_MSG_SIZE_MEDIUM,
                               op_name(TEST_OP_PUT | TEST_MSG_SIZE_MEDIUM));
    }

protected:
    /* Where the put waits when the flush or the close is issued */
    enum put_state_t {
        /* Outstanding on the held transport endpoint */
        PUT_OUTSTANDING,
        /* In the recovery queue */
        PUT_PARKED,
        /* In the recovery queue, and the failed lane does not recover */
        PUT_PARKED_LANE_DOWN
    };

    void test_held_put(put_state_t put_state, bool close_ep);

    /* Lane of a put of 'size' bytes, if one lane serves it and token failover
     * can hold it */
    ucp_lane_index_t put_lane(ucp_rkey_h rkey, size_t size)
    {
        ucp_ep_h ep             = sender().ep(0, INJECTED_EP_INDEX);
        ucp_worker_h worker     = sender().worker();
        ucp_lane_map_t am_lanes = 0;
        ucp_worker_cfg_index_t rkey_cfg_index;
        ucp_proto_select_param_t select_param;
        ucp_proto_select_elem_t *select_elem;
        ucp_proto_query_attr_t query_attr;
        ucp_proto_select_t *proto_select;
        ucp_memory_info_t mem_info;
        ucp_worker_iface_t *wiface;
        ucp_lane_index_t lane;

        if (ucs_popcount(ucp_ep_get_live_lanes(ep)) < 2) {
            UCS_TEST_SKIP_R("requires two endpoint lanes");
        }

        proto_select = ucp_proto_select_get(worker, ep->cfg_index,
                                            rkey->cfg_index, &rkey_cfg_index);
        if (proto_select == nullptr) {
            UCS_TEST_ABORT("no rkey protocol selection");
        }

        ucp_memory_info_set_host(&mem_info);
        ucp_proto_select_param_init(&select_param, UCP_OP_ID_PUT, 0, 0,
                                    UCP_DATATYPE_CONTIG, &mem_info, 1);
        select_elem = ucp_proto_select_lookup_slow(worker, proto_select, 0,
                                                   ep->cfg_index,
                                                   rkey_cfg_index,
                                                   &select_param);
        if ((select_elem == nullptr) ||
            !ucp_proto_select_elem_query(worker, select_elem, size,
                                         &query_attr)) {
            UCS_TEST_ABORT("no put protocol for " << size << " bytes");
        }

        if (ucs_popcount(query_attr.lane_map) != 1) {
            UCS_TEST_SKIP_R("put of " + std::to_string(size) + " bytes uses " +
                            std::to_string(ucs_popcount(query_attr.lane_map)) +
                            " lanes");
        }

        lane   = ucs_ffs64(query_attr.lane_map);
        wiface = ucp_worker_iface(worker, ucp_ep_get_rsc_index(ep, lane));
        if (!(wiface->attr_v2.cap.flags & UCT_IFACE_FLAG_V2_QUERY_TOKEN)) {
            UCS_TEST_SKIP_R("token failover is not supported");
        }

        /* Token failover does not hold the last AM lane */
        for (ucp_lane_index_t i = 0; i < ucp_ep_num_lanes(ep); ++i) {
            if (ucp_ep_config(ep)->key.lanes[i].lane_types &
                UCS_BIT(UCP_LANE_TYPE_AM)) {
                am_lanes |= UCS_BIT(i);
            }
        }

        if (!(am_lanes & ~UCS_BIT(lane))) {
            UCS_TEST_SKIP_R("the put lane is the only AM lane");
        }

        UCS_TEST_MESSAGE << "put of " << size << " bytes: " << query_attr.desc
                         << " on lane " << unsigned(lane);
        return lane;
    }

    /* Wait until token failover holds the failed lane. The peer has not
     * returned its token yet, so the recovery has not reposted or replayed
     * anything of this lane. */
    void wait_for_hold(ucp_ep_h ep, ucp_lane_index_t lane, uct_ep_h uct_ep)
    {
        wait_for_cond([ep, lane, uct_ep]() {
            return ucp_ep_tf_is_held(ep, lane, uct_ep);
        }, [this]() { progress(); });

        if (!ucp_ep_tf_is_held(ep, lane, uct_ep)) {
            UCS_TEST_ABORT("lane " << unsigned(lane) << " is not held");
        }

        if (ucp_ep_recovery_arg(ep)->tf[lane].purged) {
            UCS_TEST_ABORT("lane " << unsigned(lane) << " is already purged");
        }
    }

    /* Send the put while the lane has no resources, so it waits in a pending
     * queue which only the test holds, fail the lane, and wait until the hold
     * moves the put to the recovery queue */
    void *put_parked(ucp_ep_h ep, ucp_lane_index_t lane, const mem_buffer &lbuf,
                     const mapped_buffer &rbuf, ucp_rkey_h rkey, size_t size)
    {
        uct_ep_h uct_ep   = ucp_ep_get_lane(ep, lane);
        uct_iface_h iface = uct_ep->iface;
        ucs::mock purge_mock;
        ucs_status_t status;
        void *put_req;

        m_pending_uct_ep = uct_ep;
        m_pending_req    = NULL;
        m_orig_ops       = iface->ops;
        purge_mock.setup(&iface->ops.ep_pending_purge, pending_purge);

        {
            ucs::mock put_mock;
            put_mock.setup(&iface->ops.ep_put_short, put_short_no_resource);
            put_mock.setup(&iface->ops.ep_put_bcopy, put_bcopy_no_resource);
            put_mock.setup(&iface->ops.ep_put_zcopy, put_zcopy_no_resource);
            put_mock.setup(&iface->ops.ep_pending_add, pending_add);
            put_req = ucp_put_nbx(ep, lbuf.ptr(), size, uintptr_t(rbuf.ptr()),
                                  rkey, &m_req_empty_param);
        }

        if (!UCS_PTR_IS_PTR(put_req) || (m_pending_req == NULL)) {
            UCS_TEST_ABORT("put did not wait in the pending queue of lane "
                           << unsigned(lane));
        }

        EXPECT_EQ(&(static_cast<ucp_request_t*>(put_req) - 1)->send.uct,
                  m_pending_req);

        /* The hold purges the pending queue of the failed lane */
        status = uct_ep_invalidate(uct_ep, 0);
        if (status != UCS_OK) {
            UCS_TEST_ABORT("uct_ep_invalidate failed: "
                           << ucs_status_string(status));
        }

        wait_for_hold(ep, lane, uct_ep);
        EXPECT_EQ(nullptr, m_pending_req);
        EXPECT_FALSE(
                ucs_queue_is_empty(&ucp_ep_recovery_arg(ep)->tf_pending_q));
        return put_req;
    }

    /* Progress until the request completes, and release it */
    ucs_status_t wait_and_release(void *req, bool close_req)
    {
        ucs_status_t status;

        if (!UCS_PTR_IS_PTR(req)) {
            return UCS_PTR_STATUS(req);
        }

        wait_for_cond([req]() {
            return ucp_request_check_status(req) != UCS_INPROGRESS;
        }, [this]() { progress(); });

        status = ucp_request_check_status(req);
        if (status == UCS_INPROGRESS) {
            return status;
        }

        if (close_req) {
            sender().close_ep_req_free(req);
        } else {
            ucp_request_free(req);
        }

        return status;
    }

    static bool is_delivered(const mem_buffer &lbuf, const mapped_buffer &rbuf,
                             size_t size)
    {
        return mem_buffer::compare(lbuf.ptr(), rbuf.ptr(), size,
                                   UCS_MEMORY_TYPE_HOST);
    }

    static ucs_status_t
    put_short_no_resource(uct_ep_h ep, const void *buffer, unsigned length,
                          uint64_t remote_addr, uct_rkey_t rkey)
    {
        if (ep == m_pending_uct_ep) {
            return UCS_ERR_NO_RESOURCE;
        }

        return m_orig_ops.ep_put_short(ep, buffer, length, remote_addr, rkey);
    }

    static ssize_t
    put_bcopy_no_resource(uct_ep_h ep, uct_pack_callback_t pack_cb, void *arg,
                          uint64_t remote_addr, uct_rkey_t rkey)
    {
        if (ep == m_pending_uct_ep) {
            return UCS_ERR_NO_RESOURCE;
        }

        return m_orig_ops.ep_put_bcopy(ep, pack_cb, arg, remote_addr, rkey);
    }

    static ucs_status_t
    put_zcopy_no_resource(uct_ep_h ep, const uct_iov_t *iov, size_t iovcnt,
                          uint64_t remote_addr, uct_rkey_t rkey,
                          uct_completion_t *comp)
    {
        if (ep == m_pending_uct_ep) {
            return UCS_ERR_NO_RESOURCE;
        }

        return m_orig_ops.ep_put_zcopy(ep, iov, iovcnt, remote_addr, rkey,
                                       comp);
    }

    static ucs_status_t
    pending_add(uct_ep_h ep, uct_pending_req_t *req, unsigned flags)
    {
        if ((ep == m_pending_uct_ep) && (m_pending_req == NULL)) {
            m_pending_req = req;
            return UCS_OK;
        }

        return m_orig_ops.ep_pending_add(ep, req, flags);
    }

    static void
    pending_purge(uct_ep_h ep, uct_pending_purge_callback_t cb, void *arg)
    {
        uct_pending_req_t *req = m_pending_req;

        if ((ep == m_pending_uct_ep) && (req != NULL)) {
            m_pending_req = NULL;
            cb(req, arg);
        }

        m_orig_ops.ep_pending_purge(ep, cb, arg);
    }

    static uct_ep_h m_pending_uct_ep;
    static uct_pending_req_t *m_pending_req;
    static uct_iface_ops_t m_orig_ops;
};

uct_ep_h test_ucp_flush_tf_held::m_pending_uct_ep        = NULL;
uct_pending_req_t *test_ucp_flush_tf_held::m_pending_req = NULL;
uct_iface_ops_t test_ucp_flush_tf_held::m_orig_ops;

void test_ucp_flush_tf_held::test_held_put(put_state_t put_state, bool close_ep)
{
    const size_t size = rma_msg_size();
    const char *what  = close_ep ? "close" : "flush";
    ucs::mock probe_mock;
    ucs_status_t put_status;
    ucs_status_t status;
    ucp_lane_index_t lane;
    uct_ep_h uct_ep;
    void *put_req;
    void *req;
    ucp_ep_h ep;

    flush_workers();
    ep = sender().ep(0, INJECTED_EP_INDEX);

    mem_buffer lbuf(size, UCS_MEMORY_TYPE_HOST);
    mapped_buffer rbuf(size, receiver());
    ucs::handle<ucp_rkey_h> rkey = rbuf.rkey(sender());

    lane   = put_lane(rkey, size);
    uct_ep = ucp_ep_get_lane(ep, lane);
    lbuf.pattern_fill(m_seed);
    rbuf.memset(0);
    ASSERT_FALSE(is_delivered(lbuf, rbuf, size));

    if (put_state == PUT_OUTSTANDING) {
        /* The put is sent on the failed lane before the failure is detected */
        ASSERT_UCS_OK(uct_ep_invalidate(uct_ep, 0));
        put_req = ucp_put_nbx(ep, lbuf.ptr(), size, uintptr_t(rbuf.ptr()),
                              rkey, &m_req_empty_param);
        ASSERT_FALSE(UCS_PTR_IS_ERR(put_req));
        wait_for_hold(ep, lane, uct_ep);
    } else {
        if (put_state == PUT_PARKED_LANE_DOWN) {
            /* The probes of the rebuilt lane fail, so it does not recover */
            mock_recovery_probe(sender().worker(), probe_mock,
                                recovery_probe_fail);
        }

        put_req = put_parked(ep, lane, lbuf, rbuf, rkey, size);
        EXPECT_EQ(UCS_INPROGRESS, ucp_request_check_status(put_req));
    }

    ASSERT_FALSE(is_delivered(lbuf, rbuf, size));

    if (close_ep) {
        req = sender().disconnect_nb(0, INJECTED_EP_INDEX);
    } else {
        req = ucp_ep_flush_nbx(ep, &m_req_empty_param);
    }

    ASSERT_FALSE(UCS_PTR_IS_ERR(req));
    EXPECT_TRUE(UCS_PTR_IS_PTR(req))
            << what << " completed before the put sent before it";

    /* The put completes before the flush or the close, and its data is
     * delivered */
    status = wait_and_release(req, close_ep);
    EXPECT_EQ(UCS_OK, status) << what << " status "
                              << ucs_status_string(status);
    if (UCS_PTR_IS_PTR(put_req)) {
        EXPECT_NE(UCS_INPROGRESS, ucp_request_check_status(put_req))
                << "put completed after the " << what;
    }

    EXPECT_TRUE(is_delivered(lbuf, rbuf, size))
            << "put data was not delivered when the " << what << " completed";

    if (put_state == PUT_PARKED_LANE_DOWN) {
        EXPECT_TRUE(ucp_ep_get_failed_lanes(ep) & UCS_BIT(lane))
                << "lane " << unsigned(lane) << " recovered";
    }

    put_status = wait_and_release(put_req, false);
    if (put_status == UCS_INPROGRESS) {
        ADD_FAILURE() << "put did not complete";
        /* Let the lane recover, so the recovery sends the put */
        probe_mock.cleanup();
        put_status = wait_and_release(put_req, false);
    }

    EXPECT_EQ(UCS_OK, put_status) << "put status "
                                  << ucs_status_string(put_status);
    EXPECT_TRUE(is_delivered(lbuf, rbuf, size)) << "put data was not delivered";

    if ((put_state != PUT_PARKED_LANE_DOWN) && !close_ep) {
        test_recovery(TEST_OP_PUT);
    }

    EXPECT_EQ(0, total_err_count());
}

UCS_TEST_P(test_ucp_flush_tf_held, flush_outstanding_put, "MAX_EAGER_LANES=8",
           "RECOVERY_RETRIES=100")
{
    test_held_put(PUT_OUTSTANDING, false);
}

UCS_TEST_P(test_ucp_flush_tf_held, flush_parked_put, "MAX_EAGER_LANES=8",
           "RECOVERY_RETRIES=100")
{
    test_held_put(PUT_PARKED, false);
}

UCS_TEST_P(test_ucp_flush_tf_held, flush_parked_put_lane_down,
           "MAX_EAGER_LANES=8", "RECOVERY_RETRIES=1000")
{
    test_held_put(PUT_PARKED_LANE_DOWN, false);
}

UCS_TEST_P(test_ucp_flush_tf_held, close_outstanding_put, "MAX_EAGER_LANES=8",
           "RECOVERY_RETRIES=100")
{
    test_held_put(PUT_OUTSTANDING, true);
}

UCS_TEST_P(test_ucp_flush_tf_held, close_parked_put, "MAX_EAGER_LANES=8",
           "RECOVERY_RETRIES=100")
{
    test_held_put(PUT_PARKED, true);
}

UCP_INSTANTIATE_TEST_CASE_TLS(test_ucp_flush_tf_held, rcx, "rc_x")
