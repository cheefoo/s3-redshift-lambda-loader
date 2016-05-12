package com.tayo.redloader;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.encryptionsdk.AwsCrypto;
import com.amazonaws.encryptionsdk.CryptoResult;
import com.amazonaws.encryptionsdk.kms.KmsMasterKey;
import com.amazonaws.encryptionsdk.kms.KmsMasterKeyProvider;


public class KmsKeyHelper 
{
		private static final Logger logger = LoggerFactory.getLogger(KmsKeyHelper.class);

	    public static List<String> getDecryptedConfig(String keyArn, List<String> configList) throws InterruptedException 
	    {       
	     	       
	        logger.info("Getting keys");
	    	List <String>resultList = new ArrayList<String>();
	    	KmsMasterKeyProvider prov = new KmsMasterKeyProvider(keyArn);
        	logger.info("KeyArn is :-" + keyArn);
        	logger.info("Key Provider  is :-" + prov.toString());
	        
	        for (String ciphertext: configList)
	        {
	        	 CryptoResult<String, KmsMasterKey> decryptResult = new AwsCrypto().decryptString(prov, ciphertext);
	        	 logger.info("After Crypto");
	        	 resultList.add(decryptResult.getResult());
	        	// logger.info(decryptResult.getResult());
	        	 Thread.sleep(10);
	        }	        
	        logger.info("Keys obtained returnin...keys");
	        return resultList;
	    }	    	    

}
