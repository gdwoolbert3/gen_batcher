# Contributing

Suggestions, Issues, and pull requests are welcome!

## Setup

Realisitcally, this project will likely run fine on any relatively modern Erlang
and Elixir version, but this project specifies the following language versions
(see `.tool-versions`):

* Erlang OTP 26.2.5.1
* Elixir 1.17.1

One of the best ways to manage multiple Erlang/Elixir versions is
[asdf](https://github.com/asdf-vm/asdf) version manager. If you're using
`asdf-vm`, you can run the following commands to install the specified language
versions:

```bash
asdf plugin-add erlang
asdf plugin-add elixir
asdf install
```

Once you have sufficient versions of Erlang and Elixir versions installed, you
can download the development dependencies:

```bash
mix deps.get
```

Assuming everything was done correctly, you should be able to successfully run
the testing suite:

```bash
mix test
```

## Pull Request Process

1. Ensure that your code is well-formatted and that all unit tests are passing
   (there is a GitHub workflow that checks both).
2. Ensure that any new functionality is well-documented in the function/module
   docs as well as in `README.md` (if applicable).
3. Increase any relevant version numbers if applicable. This project uses
   [semantic versioning](http://semver.org/).
4. Pull Requests require merge approval from a code owner (enforced by GitHub).

## Outstanding Tasks

* Investigate using a `PartitionSupervisor` to start multiple `Task.Supervisor`
  processes for the application (see `GenBatcher.Application`).
* Investigate adding doctests to the `GenBatcher` module.

## Code of Conduct

### Our Pledge

In the interest of fostering an open and welcoming environment, we as
contributors and maintainers pledge to making participation in our project and
our community a harassment-free experience for everyone, regardless of age, body
size, disability, ethnicity, gender identity and expression, level of experience,
nationality, personal appearance, race, religion, or sexual identity and
orientation.

### Our Standards

Examples of behavior that contributes to creating a positive environment
include:

* Using welcoming and inclusive language
* Being respectful of differing viewpoints and experiences
* Gracefully accepting constructive criticism
* Focusing on what is best for the community
* Showing empathy towards other community members

Examples of unacceptable behavior by participants include:

* The use of sexualized language or imagery and unwelcome sexual attention or
advances
* Trolling, insulting/derogatory comments, and personal or political attacks
* Public or private harassment
* Publishing others' private information, such as a physical or electronic
  address, without explicit permission
* Other conduct which could reasonably be considered inappropriate in a
  professional setting

### Our Responsibilities

Project maintainers are responsible for clarifying the standards of acceptable
behavior and are expected to take appropriate and fair corrective action in
response to any instances of unacceptable behavior.

Project maintainers have the right and responsibility to remove, edit, or
reject comments, commits, code, wiki edits, issues, and other contributions
that are not aligned to this Code of Conduct, or to ban temporarily or
permanently any contributor for other behaviors that they deem inappropriate,
threatening, offensive, or harmful.

### Scope

This Code of Conduct applies both within project spaces and in public spaces
when an individual is representing the project or its community. Examples of
representing a project or community include using an official project e-mail
address, posting via an official social media account, or acting as an appointed
representative at an online or offline event. Representation of a project may be
further defined and clarified by project maintainers.

### Enforcement

Instances of abusive, harassing, or otherwise unacceptable behavior may be
reported by contacting the project owner at `gordonwoolbert3@gmail.com`. All
complaints will be reviewed and investigated and will result in a response that
is deemed necessary and appropriate to the circumstances. The project team is
obligated to maintain confidentiality with regard to the reporter of an incident.
Further details of specific enforcement policies may be posted separately.

Project maintainers who do not follow or enforce the Code of Conduct in good
faith may face temporary or permanent repercussions as determined by other
members of the project's leadership.

### Attribution

This Code of Conduct is adapted from the [Contributor Covenant][homepage],
version 1.4, available at [http://contributor-covenant.org/version/1/4][version]
