Release Process
===============

The following steps explain how to prepare and publish a release for Sleeper, by using the Maven system test suite:

1. Update CHANGELOG.md with a summary of the issues fixed and improvements made in this version.

2. Create an issue for the release, and create a branch for that issue.

3. Set the new version number using `./scripts/dev/updateVersionNumber.sh`, e.g.

```bash
VERSION=0.12.0
./scripts/dev/updateVersionNumber.sh ${VERSION}
```

4. Push the branch to GitHub and open a pull request so that the tests run. If there are any failures, fix them.

5. Get the performance figures from the nightly system tests.

There should be a cron job configured to run these nightly. Running it manually and retrieving the results is documented
in the [system tests guide](13-system-tests.md#nightly-test-scripts).

Update the performance figures in the [system tests guide](13-system-tests.md#performance-benchmarks).

6. Run a deployment of the deployAll system test to test the functionality of the system. Note that it is best to
   provide a fresh instance ID that has not been used before:

```bash
ID=<instance-id>
VPC=<your-vpc-id>
SUBNETS=<your-subnet-ids>
./scripts/test/deployAll/buildDeployTest.sh ${ID} ${VPC} ${SUBNETS}
```

The following tests can be used as a quick check that all is working correctly. They are not intended to fully test
all aspects of the system. Any changes made by pull requests should be tested by doing a system test deployment on AWS
if they are likely to either affect performance or involve changes to the way the system is deployed to AWS.

7. Test a simple query:

```bash
./scripts/utility/query.sh ${ID}
```

Choose a range query, choose 'y' for the first two questions and then choose a range such as 'aaaaaa' to 'aaaazz'.
As the data that is ingested is random, it is not possible to say exactly how many results will be returned, but it
should be in the region of 900 results.

8. Test a query that will be executed by lambda:

```bash
./scripts/utility/lambdaQuery.sh ${ID}
```

Choose the S3 results bucket option and then choose the same options as above. It should say "COMPLETED".
The first query executed by lambda is a little slower than subsequent ones due to the start-up costs. The second query
should be quicker.

9. Test a query that will be executed by lambda with the results being returned over a websocket:

```bash
./scripts/utility/webSocketQuery.sh ${ID}
```

Choose the same options as above, and results should be returned.

10. Once the above tests have been done, merge the pull request into main. Then checkout the main branch,
    set the tag to `v${VERSION}` and push the tag using `git push --tags`.
