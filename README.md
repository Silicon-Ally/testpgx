_This package was developed by [Silicon Ally](https://siliconally.org) while
working on a project for  [Adventure Scientists](https://adventurescientists.org).
Many thanks to Adventure Scientists for supporting [our open source
mission](https://siliconally.org/policies/open-source/)!_

NOTE: this package is pre-v1.0.0 release, meaning that the API is liable
to change as we add + refactor functionality. Since it's only for use in
testing, that's likely fine for small projects, but
[contact us](https://siliconally.org/contact/) before integrating this
into any larger/more serious projects.

# testpgx

[![GoDoc](https://pkg.go.dev/badge/github.com/Silicon-Ally/testpgx?status.svg)](https://pkg.go.dev/github.com/Silicon-Ally/testpgx?tab=doc)
[![CI Workflow](https://github.com/Silicon-Ally/testpgx/actions/workflows/test.yml/badge.svg)](https://github.com/Silicon-Ally/testpgx/actions?query=branch%3Amain)

`testpgx` is a testing library for running tests against a real Postgres
database. It provides a test with a `*pgx.Conn` connection to a live database
running in a Docker container, though this can be converted into a more generic
`*sql.DB`.

## Usage

See [the `example/` directory](/example/) for an example of how to integrate
`testpgx` into a test suite.

## Contributing

Contribution guidelines can be found [on our website](https://siliconally.org/oss/contributor-guidelines).
