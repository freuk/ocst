#!/usr/bin/env ocaml
#use "topfind"
#require "topkg"
open Topkg

let () =
  Pkg.describe 
    ~distrib:(Pkg.distrib ()) "ocs" 
  @@ fun c ->
  Ok [ Pkg.bin "src/simulator" ~dst:"ocs";
       Pkg.bin "src/perturbator" ~dst:"ocs-perturbator";
       Pkg.bin "src/sampler" ~dst:"ocs-sampler";
       Pkg.bin "src/subtrace" ~dst:"ocs-subtrace";
       Pkg.bin "src/swftk" ~dst:"ocs-swftk";
       Pkg.bin "src/trimmer" ~dst:"ocs-trimmer";
  ]

