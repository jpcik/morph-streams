package org.rsp.jena;

import java.util.ArrayList;

import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.vocabulary.VCARD;

public class JavaDemo {
	
	public static void test1(){
		String personURI = "http://somewhere/JohnSmith";
		Model model = ModelFactory.createDefaultModel();
		model.createResource(personURI).addProperty(VCARD.FN,"John Smith");
	    model.write(System.out,"TTL");

	}

	public static void test2(){
		String personURI = "http://somewhere/JohnSmith";
		String givenName = "John";
		String familyName = "Smith";
		String fullName = givenName + " " + familyName;
		Model model = ModelFactory.createDefaultModel();
		model.createResource(personURI)
		            .addProperty(VCARD.FN,fullName)
					.addProperty(VCARD.N, model.createResource()
							                  .addProperty(VCARD.Given,givenName)
									          .addProperty(VCARD.Family,familyName));
		
		
	}
	
	public static void test3(){
		String personURI = "http://somewhere/JohnSmith";
		String givenName = "John";
		String familyName = "Smith";
		String fullName = givenName + " " + familyName;

		Model model = ModelFactory.createDefaultModel();
		model.createResource(personURI)
        .addProperty(VCARD.FN,fullName)
        .addProperty(VCARD.N,"Smitho")
		.addProperty(VCARD.N, model.createResource()
				                  .addProperty(VCARD.Given,givenName)
						          .addProperty(VCARD.Family,familyName));
		ArrayList<String> names = new ArrayList<String>(); 
	    NodeIterator iter=model.listObjectsOfProperty(VCARD.N) ;
	    while (iter.hasNext()){
	    	RDFNode obj=iter.next();
	    	if (obj.isResource())
	          names.add(obj.asResource().getProperty(VCARD.Family).getObject().toString());
	    	else if (obj.isLiteral())
	    	  names.add(obj.asLiteral().getString());
	    }
	}
	
	public static void main(String[] args){
	  test3();
	}
}
