OCS — Ocaml EASY-Backfilling simulator. 
-------------------------------------------------------------------------------

OCS is a lightweight EASY-backfilling simulator written in Ocaml.
It supports tuning the Primary and Backfilling queues and
running bandit algorithms.

OCS is distributed under the ISC license.

Homepage: http://freux.fr/ocs  

## Installation

Obandit can be installed with `opam`:

    opam install ocs

If you don't use `opam` consult the [`opam`](opam) file for build
instructions.

## Documentation

The documentation and API reference is generated from the source
interfaces. It can be consulted [online][doc] or via `odig doc
ocs`.

[doc]: http://freux.fr/ocs/doc

## Sample programs

If you installed Obandit with `opam` sample programs are located in
the directory `opam var ocs:doc`.

In the distribution sample programs and tests are located in the
[`test`](test) directory of the distribution. They can be built and run
with:

    topkg build --tests true && topkg test 
