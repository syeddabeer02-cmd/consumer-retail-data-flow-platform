# Optional Jenkins and GitLab execution

GitHub Actions is the primary executed pipeline. The Jenkinsfile and
.gitlab-ci.yml provide equivalent lint, unit/integration tests, a small pipeline
run, and replay/failure-protection checks. They do not deploy cloud resources.
They run the retail implementation; Fashion Sense CI still needs repository
review. Neither alternate pipeline has been executed on its target service here.

Jenkins needs a controller with Pipeline, Docker Pipeline and JUnit plugins,
a trusted Docker-capable agent, and a Multibranch Pipeline or Pipeline from SCM
pointing at this repository's Jenkinsfile. Its Python container installs Java17
and project dependencies; install commands run as root only inside that disposable
build container. Do not run untrusted contributions on a privileged shared agent.
Test reports and successful publication manifests are archived.

GitLab needs a mirrored/imported repository with .gitlab-ci.yml and an eligible
Linux Docker/Kubernetes runner able to run the image's root install commands.
Configure mirroring in the account; it is not enabled by these files. The validate
job runs the same commands and retains JUnit/publication artifacts for seven days.
Available runner minutes and account eligibility must be checked before use.

After connecting a service, trigger a build, verify tests and archived manifests,
and record its run URL. Adding a configuration file alone is not evidence that
Jenkins or GitLab executed successfully.
