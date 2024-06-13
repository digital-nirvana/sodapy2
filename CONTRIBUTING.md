# Contributing

This project adheres to the [Contributor Covenant Code of Conduct](http://contributor-covenant.org/version/1/4/). By participating, you are expected to honor this code.





## Getting started

The best way to start developing this project is to set up a [virtualenv](https://virtualenv.pypa.io/en/stable/) and install the requirements.
The `setup.sh` script helpfully does all this for you.

    git clone <my remote url/sodapy2.git>
    cd sodapy2
    ./setup.sh

Run tests to confirm that everything is set up properly.

    source .venv/bin/activate
    python3 -m pytest

## Submitting a pull request

1. Fork this repository
2. Create a branch: `git checkout -b my_feature`
3. Make changes
4. Run `black sodapy2 tests` to ensure that your changes conform to the coding style of this project
5. Commit: `git commit -am "Great new feature that closes #3"`. Reference any related issues in the first line of the commit message.
6. Push: `git push origin my_feature`
7. Open a pull request
8. Pat yourself on the back for making an open source contribution :)

## Packaging, versioning, and distribution

This package uses [semantic versioning](https://semver.org/).

<!-- TODO: Review, revise, and uncomment once packaging is back in business.

    Source and wheel distributions are available on PyPI. Here is how I create those releases.

    python3 setup.py bdist_wheel
    python3 setup.py sdist
    twine upload dist/*
-->

## Other considerations

- Please review the open issues before opening a PR.
- Don't forget to review and update [`README.md`](https://github.com/digital-nirvana/sodapy2/blob/main/README.md) with necessary changes.
- Ditto for [`CHANGELOG.md`](https://github.com/digital-nirvana/sodapy2/blob/main/CHANGELOG.md).
- Writing tests is never a bad idea. Make sure all tests are passing before opening a PR.
