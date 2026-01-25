{ ... }: rec {
  mkGritPipeline =
    { name
    , steps ? [ ]
    }:
    builtins.concatStringsSep "\n" steps;

  mkStep =
    args@{ name
    , isStart ? false
    , script
    , parallel
    , inputs ? [ ]
    ,
    }:
    let
      isAttrSet = a: builtins.hasAttr a args;
    in
    ''
[[step]]
name="${name}"
script='''
${script}
'''
${if isStart then "start=true" else ""}
${if ( isAttrSet "parallel" ) then "parallel=${builtins.toString parallel}" else ""}
${if ( isAttrSet "inputs" ) then "inputs=[${builtins.concatStringsSep ", " (builtins.map (x: "\"${x}\"") inputs)}]" else ""}
    '';

  version = "1.0";
}
