## How to contribute to Sleeper

### Raising issues

You can raise feature requests or bugs as issues in GitHub. Please fill in all relevant information.

Before submitting an issue, please ensure you search the existing issues to check if your change has already been
requested. Comment on any issue to let us know you're interested.

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