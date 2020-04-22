/**
* Copyright (C) Mellanox Technologies Ltd. 2001-2014.  ALL RIGHTS RESERVED.
* Copyright (C) UT-Battelle, LLC. 2014. ALL RIGHTS RESERVED.
* Copyright (C) ARM Ltd. 2016.  ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/

#include <common/test.h>
extern "C" {
#include <ucs/time/timerq.h>
}

#include <time.h>

class test_time : public ucs::test {
};

UCS_TEST_F(test_time, time_calc) {
    double value = ucs::rand() % UCS_USEC_PER_SEC;

    EXPECT_NEAR(value * 1000ull, ucs_time_to_msec(ucs_time_from_sec (value)), 0.000001);
    EXPECT_NEAR(value * 1000ull, ucs_time_to_usec(ucs_time_from_msec(value)), 0.01);
    EXPECT_NEAR(value * 1000ull, ucs_time_to_nsec(ucs_time_from_usec(value)), 20.0);
}

/* This test is only useful when used with high-precision timers */
#if HAVE_HW_TIMER
UCS_TEST_SKIP_COND_F(test_time, get_time,
                     (ucs::test_time_multiplier() > 1)) {
    ucs_time_t time1 = ucs_get_time();
    ucs_time_t time2 = ucs_get_time();
    EXPECT_GE(time2, time1);

    ucs_time_t start_time = ucs_get_time();
    ucs_time_t end_time = start_time + ucs_time_from_sec(1);
    ucs_time_t current_time;

    time_t system_start_time = time(NULL);

    uint64_t count = 0;
    do {
        current_time = ucs_get_time();
        ++count;
    } while (current_time <= end_time);

    /* Check the sleep interval is correct */
    if (ucs::perf_retry_count) {
        ASSERT_NEAR(1.0, time(NULL) - system_start_time, 1.00001);

        double nsec = (ucs_time_to_nsec(current_time - start_time)) / count;
        EXPECT_LT(nsec, 40.0) << "ucs_get_time() performance is too bad";
    }
}
#endif

UCS_TEST_F(test_time, timerq) {
    static const int TIMER_ID_1  = 100;
    static const int TIMER_ID_2  = 200;

    ucs_timer_queue_t timerq;
    ucs_status_t status;

    for (unsigned test_count = 0; test_count < 500; ++test_count) {

        const ucs_time_t interval1 = (ucs::rand() % 20) + 1;
        const ucs_time_t interval2 = (ucs::rand() % 20) + 1;
        const ucs_time_t test_time = ucs::rand() % 10000;
        const ucs_time_t time_base = ucs::rand();
        ucs_timer_t *timer;
        unsigned counter1, counter2;

        status = ucs_timerq_init(&timerq);
        ASSERT_UCS_OK(status);

        EXPECT_TRUE(ucs_timerq_is_empty(&timerq));
        EXPECT_EQ(UCS_TIME_INFINITY, ucs_timerq_min_interval(&timerq));

        ucs_time_t current_time = time_base;

        ucs_timerq_add(&timerq, TIMER_ID_1, interval1);
        ucs_timerq_add(&timerq, TIMER_ID_2, interval2);

        EXPECT_FALSE(ucs_timerq_is_empty(&timerq));
        EXPECT_EQ(std::min(interval1, interval2), ucs_timerq_min_interval(&timerq));

        /*
         * Check that both timers are invoked
         */
        counter1 = 0;
        counter2 = 0;
        for (unsigned count = 0; count < test_time; ++count) {
            ++current_time;
            ucs_timerq_for_each_expired(NULL, timer, &timerq, current_time, {
                if (timer->id == TIMER_ID_1) ++counter1;
                if (timer->id == TIMER_ID_2) ++counter2;
            })
        }
        EXPECT_NEAR(test_time / interval1, counter1, 1);
        EXPECT_NEAR(test_time / interval2, counter2, 1);

        /*
         * Check that after canceling, only one timer is invoked
         */
        counter1 = 0;
        counter2 = 0;
        status = ucs_timerq_remove(&timerq, TIMER_ID_1);
        ASSERT_UCS_OK(status);
        for (unsigned count = 0; count < test_time; ++count) {
            ++current_time;
            ucs_timerq_for_each_expired(NULL, timer, &timerq, current_time, {
                if (timer->id == TIMER_ID_1) ++counter1;
                if (timer->id == TIMER_ID_2) ++counter2;
            })
        }
        EXPECT_EQ(0u, counter1);
        EXPECT_NEAR(test_time / interval2, counter2, 1);
        EXPECT_EQ(interval2, ucs_timerq_min_interval(&timerq));

        /*
         * Check that after rescheduling, both timers are invoked again
         */
        ucs_timerq_add(&timerq, TIMER_ID_1, interval1);

        counter1 = 0;
        counter2 = 0;
        for (unsigned count = 0; count < test_time; ++count) {
            ++current_time;
            ucs_timerq_for_each_expired(NULL, timer, &timerq, current_time, {
                if (timer->id == TIMER_ID_1) ++counter1;
                if (timer->id == TIMER_ID_2) ++counter2;
            })
        }
        EXPECT_NEAR(test_time / interval1, counter1, 1);
        EXPECT_NEAR(test_time / interval2, counter2, 1);

        status = ucs_timerq_remove(&timerq, TIMER_ID_1);
        EXPECT_UCS_OK(status);
        status = ucs_timerq_remove(&timerq, TIMER_ID_2);
        EXPECT_UCS_OK(status);

        /*
         * Check that can use the array of timer IDs acquired from
         * timerq after the timers were removed
         */
        ucs_timerq_add(&timerq, TIMER_ID_1, interval1);
        ucs_timerq_add(&timerq, TIMER_ID_2, interval2);

        size_t timerq_size = ucs_timerq_size(&timerq);
        EXPECT_EQ(2lu, timerq_size);
        int *timer_ids = NULL, *mem = NULL;
        unsigned count = 0;
        counter1 = 0;
        counter2 = 0;
        for (;;) {
            ++count;
            ++current_time;
            ucs_timerq_for_each_expired(&mem, timer, &timerq, current_time, {
                if (mem != NULL) {
                    EXPECT_EQ(1u, count);
                    timer_ids = mem;
                    memset(timer_ids, 0, timerq_size * sizeof(*timer_ids));
                    mem = NULL;
                }

                EXPECT_TRUE(timer_ids != NULL);
                if (timer_ids != NULL) {
                    if (timer->id == TIMER_ID_1) {
                        timer_ids[0] = TIMER_ID_1;
                        ++counter1;
                    }

                    if (timer->id == TIMER_ID_2) {
                        timer_ids[1] = TIMER_ID_2;
                        ++counter2;
                    }
                }
            })

            EXPECT_TRUE(timer_ids != NULL);
            if (timer_ids == NULL) {
                break;
            }

            if ((timer_ids[0] == TIMER_ID_1) && (timer_ids[1] == TIMER_ID_2)) {
                break;
            }
        }
        EXPECT_NEAR(count / interval1, counter1, 1);
        EXPECT_NEAR(count / interval2, counter2, 1);

        status = ucs_timerq_remove(&timerq, TIMER_ID_1);
        EXPECT_UCS_OK(status);
        status = ucs_timerq_remove(&timerq, TIMER_ID_2);
        EXPECT_UCS_OK(status);

        bool found_id1 = false, found_id2 = false;

        if (timer_ids != NULL) {
            for (size_t i = 0; i < timerq_size; i++) {
                if (TIMER_ID_1 == timer_ids[i]) {
                    found_id1 = true;
                }

                if (TIMER_ID_2 == timer_ids[i]) {
                    found_id2 = true;
                }
            }
        }

        EXPECT_TRUE(found_id1);
        EXPECT_TRUE(found_id2);

        ucs_timerq_release_timer_ids_mem(&timerq, timer_ids);

        ucs_timerq_cleanup(&timerq);
    }
}
