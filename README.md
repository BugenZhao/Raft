# Raft

[![Test](https://github.com/BugenZhao/Raft/actions/workflows/test.yaml/badge.svg)](https://github.com/BugenZhao/Raft/actions/workflows/test.yaml)
[![Docs](https://img.shields.io/badge/Docs-detailed-orange)](https://bugenzhao.com/Raft/)

A solution to _"6.824 Lab 2: Raft"_ and _"6.824 Lab 3: Fault-tolerant Key/Value Service"_ from MIT, with detailed documentations. Lab of Percolator transaction model is also included.

Base code and lab parts marked with `*` are migrated from [pingcap/talent-plan](https://github.com/pingcap/talent-plan).

## Roadmap

- [x] Lab 2
  - [x] Part 2A: leader election \*
  - [x] Part 2B: log \*
  - [x] Part 2C: persistence \*
  - [x] Part 2D: log compaction
- [x] Lab 3
  - [x] Part 3A: Key/value service without snapshots \*
  - [x] Part 3B: Key/value service with snapshots (\*)
- [x] Percolator \*

# Distributed Systems in Rust

A training course about the distributed systems in [Rust].

Subjects covered include:

- [Raft consensus algorithm] (including a fault-tolerant key-value storage service
  using Raft)
- [Percolator transaction model]

After completing this course you will have the knowledge to implement a basic
key-value storage service with transaction and fault-tolerant in Rust.

**Important note: Distributed Systems in Rust is in an alpha state**
It might contain bugs. Any feedback is greatly appreciated. Please [file issues]
if you have any problem. And also You are encouraged to fix problems on your own
and submit pull requests.

## The goal of this course

The goal of this course is to teach the Rust programmers who are interested in
distributed systems to know about how to make the distributed systems reliable
and how to implement the distributed transaction.

## Who is this for?

Distributed Systems in Rust is for experienced _Rust_ programmers, who are
familiar with the Rust language. If you are not, you can first learn our [rust]
lessons.

## A PingCAP-specific note

This course, combined with [Deep Dive TiKV], is intended to be enough to enable
programmers to meaningfully contribute to [TiKV]. It is most specifically
designed to teach those in the Chinese Rust community enough Rust to work on
TiKV. The language used is intended to be simple so that those who read only a
little English can follow. If you find any of the language difficult to
understand please [file issues].

## License

[CC-BY 4.0](https://opendefinition.org/licenses/cc-by/)

<!-- links -->

[rust]: ../rust/README.md
[file issues]: https://github.com/pingcap/talent-plan/issues/
[deep dive tikv]: https://tikv.github.io/deep-dive-tikv/overview/introduction.html
[tikv]: https://github.com/tikv/tikv/
[rust]: https://www.rust-lang.org/
[raft consensus algorithm]: raft/README.md
[percolator transaction model]: percolator/README.md
