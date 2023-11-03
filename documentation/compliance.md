## Numbered requirements

Sketch expects:

1. The program to be written in a real programming language like Python, Ruby, Go, Elixir, etc… Please, avoid shell scripting or similar. We don't expect a code masterpiece but we expect clean code that is easy to understand and modify without major refactors.
2. The program to run in a POSIX-compliant environment.
3. To be able to test and run the program with minimal setup/configuration.
4. Documentation in the form of comments in the code and a README file explaining the function and how to run your solution.
5. The program to handle most common problems. It should not crash if something unexpected happens and it should be able to resume if it stops at the middle.
6. The program to be fast and efficient; assume the buckets store hundreds of millions of images, and we want to move them in the shortest time possible. Please provide comments about performance and scalability when you send the challenge back to us.
7. The program to be able to run in a production environment. Meaning that the information located in the database must always point to an existing PNG in S3 and not cause any service disruption.
8. You to write a few lines explaining your development setup (how did you create or emulate the resources).
9. The code of the challenge written in a Git repository, zipped and uploaded to Ashby. Try to not write the entire program in one commit and version it as much as you can. For us, understanding your progress is valuable.
10. A description of your deployment plan to production. We don't need to see any IaC, we only want a written explanation.
11. A comment on: _"you can use any S3 and database user you want, but it would great that you describe what privileges the PostgreSQL user and the S3 user needs to have to be able to perform the operations needed by your program"_

## Statement of compliance

The delivery is such that:

1. The program is written in Python and is compliant with a cherry-picked subset of PEP8. Configuration is separated from code and functions are relatively small, providing appropriate procedural abstration.
2. The program has been prepared for execution on Ubuntu LTS without any external dependency.
3. The program is delivered with a helper script that automatically prepares a test environment and with easy to use markdown documentation. A simple integration test, that validates sucessful execution and measures performance, is also delivered.
4. All delivered components (sketch_prepare.py, sketch_migrate.py and sketch_test.py) have a companion README file. The code contains contextual comments.
5. The most common problems, including wrong hostnames, wrong resource names, wrong credentials and bad inputs have been simulated and gracefully handled in the code.
6. The solution inclues a nearby Linux instance. The program avoids overwriting previously copied files and applies parallelization using the python multiprocessing module.
7. The program does not change any database entry until the corresponding file has been copied between buckets.
8. The following resources were created on a Digital Ocean cloud account: PostegrSQL database, Ubuntu Linux droplet, S3 compatible buckets.
9. The repository was created early in the process. The evolution can be tracked by reviewing the commits and merges to the dev branch and the (less numerous) merges to the master branch and the corresponding 0.9.X tags.
10. The deployment is simple as it only requires the cloning of the repository and the installation of some apt packages from the Ubuntu distribution. The presence of the repository and the apt packages can be ensured, in a reproducible way, by means of configuration management using Puppet, Ansible or other equivalent tool. The Linux machine can be created using Terraform, Cloudformation or an equivalent solution.
11. The S3 user needs reading privileges on the legacy bucket and read-write privileges on the production bucket. The database user needs SELECT and UPDATE(path) privileges on the database - a database user with such limited privileges is created by the environment preparation script and has been sucessfully used with the migration script.

A presentation, where the points above are described in more detail, will be provided to the Sketch team.
