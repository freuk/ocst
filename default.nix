with import <nixpkgs> {};
with import ../obps {};
stdenv.lib.overrideDerivation ocs (oldAttrs : {
  src = ./.;
})
