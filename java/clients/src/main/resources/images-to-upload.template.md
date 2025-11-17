## Docker Deployment Images
A build of Sleeper outputs several directories under `scripts/docker`. Each is the directory to build a Docker image, with a Dockerfile.
Some of these are used for parts of Sleeper that are always deployed from Docker images, and those are listed here.
* Deployment name - This is both the name of its directory under `scripts/docker`, and the name of the image when it's built and the repository it's uploaded to.
* Optional Stack - They're each associated with an optional stack, and will only be used when that optional stack is deployed in an instance of Sleeper.
* Multiplatform - Compaction job execution is built as a multiplatform image, so it can be deployed in both x86 and ARM architectures.


%DOCKER_IMAGES_TABLE%
## Lambda Deployment Images
These are all used with the Docker build directory that's output during a build of Sleeper at `scripts/docker/lambda`.
Most lambdas are usually deployed from a jar in the jars bucket, but some need to be deployed as a Docker container, and we have the option to deploy all lambdas as Docker containers as well.
To build a Docker image for a lambda, we copy its jar file from `scripts/jars` to `scripts/docker/lambda/lambda.jar`, and then run the Docker build for that directory.
This results in a separate Docker image for each lambda jar.
* Filename - This is the name of the jar file that's output by the build in `scripts/jars`.
It includes the version number you've built, which we've included as a placeholder here.
* Image name - This is the name of the Docker image that's built, and the name of the repository it's uploaded to.
* Always Docker deploy - This means that that lambda will always be deployed with Docker, usually because the jar is too large to deploy directly.


%LAMBDA_IMAGES_TABLE%
