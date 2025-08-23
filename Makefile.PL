use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
  NAME         => 'Twom',
  VERSION_FROM => 'lib/Twom.pm',

  # Ensure xsubpp runs: Twom.xs -> Twom.c
  XS           => { 'Twom.xs' => 'Twom.c' },

  # Also compile your raw C file
  C            => [ 'twom.c' ],

  # Let MakeMaker link all objects (XS + twom.o). Default is $(O_FILES).
  OBJECT     => '$(O_FILES) Twom.o',   # optional; you can omit this line

  INC          => '-I.',         # so #include "twom.h" works
  LIBS         => ['-luuid'],          # no external libs
  # DEFINE     => '-std=c99',    # uncomment if your C needs C99
);
