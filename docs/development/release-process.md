Release Process
===============

The following steps explain how to prepare and publish a release for Sleeper.

1. Create the following issues in GitHub:
   - Update changelog for `<version>`
     - Update [CHANGELOG.md](../../CHANGELOG.md) with a summary of the issues fixed and improvements made in this version.
   - Update roadmap for `<version>`
     - Update the [roadmap](roadmap.md) to remove any planned features that have been implemented in this release, and
       give an up to date view of upcoming work.
   - Prepare release `<version>`
     - Set the version number using `scripts/dev/updateVersionNumber.sh <version>`.
     - Update the performance figures in the [system tests guide](system-tests.md#performance-benchmarks).
   - Update version number to `<next-version>-SNAPSHOT`
     - This will be done immediately after the release.

2. Read through the documentation to find anything that may need updating, and raise issues to document features or
   design that has changed in this release.

3. Make sure the [NOTICES](../../NOTICES) file is up to date.

4. Create pull requests for the changelog and roadmap update. These can stay on hold until the release is ready. The
   changelog should be updated with any bug fixes or further changes.

5. Review the output of nightly system tests. If there are any failures, fix them. This should be done daily, regardless
   of release.

There should be a cron job configured to run the test suite nightly. This setup is documented in
the [system tests guide](system-tests.md#nightly-test-scripts).

At this point we can pause merging any pull requests that are not essential for the release. This includes version
upgrades done by Dependabot.

6. Run a deployment of the deployAll system test to test the functionality of the system. Note that it is best to
   provide a fresh instance ID that has not been used before:

```bash
ID=<instance-id>
VPC=<your-vpc-id>
SUBNETS=<your-subnet-ids>
./scripts/test/deployAll/buildDeployTest.sh ${ID} ${VPC} ${SUBNETS}
```

This should start a number of ECS tasks in the system test cluster for the instance. These will ingest some random data
for testing, so wait for them to finish. You can use the AWS console or CLI to check this.

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

10. Ensure you have an up to date run of the performance test suite run against the latest commit, and get the
    performance figures from the test output. Ideally this can come from the nightly system tests, but it may be
    necessary to re-run the tests manually. See the [system test guide](system-tests.md#acceptance-tests).

11. Create a pull request for release preparation, with the performance figures and version number update.

12. Once the above tests are complete and everything passes, merge the pull requests for the changelog, roadmap update
    and release preparation.

13. Raise a pull request from the develop branch to the main branch. Once the build passes, merge the pull request into
    main. Then checkout the main branch, set the tag to `v${VERSION}` and push the tag using `git push --tags`.

14. Create a pull request to update the version number for the next release as a snapshot version. Merge it when
    everything passes.
