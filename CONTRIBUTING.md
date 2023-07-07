## How to contribute to Sleeper

### Raising issues

You can raise feature requests or bugs as issues in GitHub. For bugs, please ensure you include steps to reproduce the
problem. For feature requests, please give as much information as you can on why you want the change and what value it
represents to you.

Before submitting an issue, please ensure you search the existing issues to check if your change has already been
requested.

### Contributing code

TODO

### Testing strategy

#### Manual

#### Acceptance automation

We have system tests in scripts/test that help determine whether the system works when deployed, and whether the
performance is acceptable.
We have a test script that runs all system tests in one go, which we run nightly against the main branch.
The script uploads the results of each test to an S3 bucket. This can be
found [here](./scripts/test/nightly/runTests.sh).

#### Integration

#### Unit tests

### Linting

### Architectural strategy

### Pull requests strategy

#### How to sign the CLA

### Issue strategy

### Types of issue

#### What should be included on each one

#### How do we use them