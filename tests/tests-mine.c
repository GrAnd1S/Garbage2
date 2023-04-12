/*
 * You can use this file for your own tests
 */

#include "libs/cut.h"
#include "libs/mainwrap.h"
#include "libs/utils.h"

#include <stdlib.h>

/* The following “extentions” to CUT are available in this test file:
 *
 * • ‹CHECK_IS_EMPTY(file)› — test whether the file is empty.
 * • ‹CHECK_NOT_EMPTY(file)› — inverse of the above.
 *
 * • ‹app_main_args(ARG…)› — call your ‹main()› with given arguments.
 * • ‹app_main()› — call your ‹main()› without any arguments. */

TEST(my_test)
{
    int rv = 0; /* return value of main()*/
    CHECK(app_main_args("FILE" /*, arguments for main() */) == rv);

    /* TIP: Use ‹app_main()› to test the program without arguments. */

    const char *correct_output =
        "Output on STDOUT"
    ;

    ASSERT_FILE(stdout, correct_output);
    CHECK_FILE(stderr, "" /* STDERR is empty*/);
}
