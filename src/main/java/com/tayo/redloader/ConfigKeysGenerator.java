package com.tayo.redloader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import com.amazonaws.encryptionsdk.AwsCrypto;
import com.amazonaws.encryptionsdk.kms.KmsMasterKeyProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

public class ConfigKeysGenerator {

	public static void main(String[] args) 
	{
		if(args.length!= 6)
		{
			System.out.println("Required 6 arguments: key_arn aws_region access_key access_secret_key redshift_username redshist_user_passwrod");
			System.out.println("Usage: ConfigKeysGenerator key_arn aws_region access_key access_secret_key redshift_username redshist_user_passwrod");
			
			System.exit(1);
		}
		
        final AwsCrypto crypto = new AwsCrypto();
        final KmsMasterKeyProvider prov = new KmsMasterKeyProvider(args[0]);
        
        prov.setRegion(Region.getRegion(Regions.valueOf(args[1].toUpperCase())));
        
        final String access_key = crypto.encryptString(prov, args[2]).getResult();
        final String access_secret_key = crypto.encryptString(prov, args[3]).getResult();
        final String redshift_username = crypto.encryptString(prov, args[4]).getResult();
        final String redshift_password = crypto.encryptString(prov, args[5]).getResult();

       /* System.out.println("access_key=" +access_key);
        System.out.println("access_secret_key=" +access_secret_key);
        System.out.println("redshift_username=" +redshift_username);
        System.out.println("access_key=" +redshift_password); */
        
        File file = new File("redl0ader_ciphertext.txt");
        try
        {
        	if(!file.exists())
            {
            	file.createNewFile();
            }
            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write("access_key="+access_key);
            bw.newLine();
            bw.write("access_secret_key="+access_secret_key);
            bw.newLine();
            bw.write("redshift_username="+redshift_username);
            bw.newLine();
            bw.write("redshift_password="+redshift_password);
            bw.newLine();
            bw.close();
            System.out.println("Done");

            

        	
        }
        catch(IOException ioe)
        {
          System.out.println(ioe.toString());
        }
        
        
        
	}

}
