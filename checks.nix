{ pkgs, grit }:
let
  mkTest = { name, test }: pkgs.runCommand name { } test;
in
{

  nixBuildersWork = mkTest {
    name = "nixBuildersWork";

    test =
      let
        step1 = grit.mkStep { name = "test"; script = "echo hi"; parallel = 1; };
        pipeline = grit.mkGritPipeline { name = "testpipeline"; steps = [ step1 ]; };
        expected = ''
[[step]]
name="test"
script='''
echo hi
'''

parallel=1

        '';
      in
      ''
        if [ "${pipeline}" = "${expected}" ]; then
          mkdir -p $out
          echo "Test passed" > $out/result
        else
          echo "Test failed"
          echo "Expected:"
          echo "${expected}"
          echo "Got:"
          echo "${pipeline}"
          exit 1
        fi
      '';
  };
}
