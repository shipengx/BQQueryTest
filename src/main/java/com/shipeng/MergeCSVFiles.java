package com.shipeng;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class MergeCSVFiles {

    public static void main(String[] args) {

        //in mac oxs
        String command = "sh -c cat /opt/opinmind/var/cli/nzTmpDir/cli_test_00000000000*.csv > /opt/opinmind/var/cli/nzTmpDir/udcu.nz.5874183280693280516.csv";

        String[] commands = {"bash", "-c", "cat /opt/opinmind/var/cli/nzTmpDir/cli_test_00000000000*.csv > /opt/opinmind/var/cli/nzTmpDir/udcu.nz.5874183280693280516.csv"};

        String s;
        Process p;
        try {
            p = Runtime.getRuntime().exec(commands);
            BufferedReader br = new BufferedReader(
                    new InputStreamReader(p.getInputStream()));
            while ((s = br.readLine()) != null)
                System.out.println("line: " + s);
            p.waitFor();
            System.out.println ("exit: " + p.exitValue());
            p.destroy();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String executeCommand(String command) {

        StringBuffer output = new StringBuffer();

        Process p;
        try {
            p = Runtime.getRuntime().exec(command);
            p.waitFor();
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(p.getInputStream()));

            String line = "";
            while ((line = reader.readLine())!= null) {
                output.append(line + "\n");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return output.toString();

    }

}
