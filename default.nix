{ mkDerivation, base, bytestring, conduit, containers, lifted-base
, monad-control, monad-logger, mtl, persistent, resourcet, safe
, split, stdenv, text, transformers
}:
mkDerivation {
  pname = "persistent-mysql-extra";
  version = "0.0.6";
  src = ./.;
  buildDepends = [
    base bytestring conduit containers lifted-base monad-control
    monad-logger mtl persistent resourcet safe split text transformers
  ];
  homepage = "https://github.com/circuithub/persistent-mysql-extra";
  description = "MySQL-specific queries for use with persistent";
  license = stdenv.lib.licenses.mit;
}
