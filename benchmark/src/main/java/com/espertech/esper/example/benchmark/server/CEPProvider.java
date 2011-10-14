/**************************************************************************************
 * Copyright (C) 2007 Esper Team. All rights reserved.                                *
 * http://esper.codehaus.org                                                          *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.example.benchmark.server;

import static es.upm.fi.dia.oeg.common.ParameterUtils.loadQuery;

import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;

import com.espertech.esper.client.*;

import com.espertech.esper.example.benchmark.MarketData;

import es.upm.fi.dia.oeg.common.ParameterUtils;
import es.upm.fi.dia.oeg.integration.DataSourceException;
import es.upm.fi.dia.oeg.integration.IntegratorConfigurationException;
import es.upm.fi.dia.oeg.integration.QueryCompilerException;
import es.upm.fi.dia.oeg.integration.QueryDocument;
import es.upm.fi.dia.oeg.integration.QueryException;
import es.upm.fi.dia.oeg.integration.SemanticIntegrator;
import es.upm.fi.dia.oeg.integration.Statement;
import es.upm.fi.dia.oeg.integration.metadata.PullDataSourceMetadata;
import es.upm.fi.dia.oeg.integration.registry.IntegratorRegistryException;

import es.upm.fi.oeg.integration.adapter.esper.EsperAdapter;
import es.upm.fi.oeg.integration.adapter.esper.EsperListener;
import es.upm.fi.oeg.integration.adapter.esper.EsperStatement;

/**
 * A factory and interface to wrap ESP/CEP engine dependency in a single space
 *
 * @author Alexandre Vasseur http://avasseur.blogspot.com
 */
public class CEPProvider {

    public static interface ICEPProvider {

        public void init(int sleepListenerMillis);

        public void registerStatement(String statement, String statementID);

        public void queryFactory();

        public void sendEvent(Object event);
    }

    public static ICEPProvider getCEPProvider() {
        String className = System.getProperty("esper.benchmark.provider", EsperCEPProvider.class.getName());
        try {
            Class klass = Class.forName(className);
            return (ICEPProvider) klass.newInstance();
        } catch (Throwable t) {
            t.printStackTrace();
            throw new RuntimeException(t);
        }
    }

    public static class EsperCEPProvider implements ICEPProvider {

        //private EPAdministrator epAdministrator;

        //private EPRuntime epRuntime;
        private EsperAdapter esper;
        private SemanticIntegrator si;
        
        // only one of those 2 will be attached to statement depending on the -mode selected
        //private UpdateListener updateListener;
        //private MySubscriber subscriber;
        private EsperListener listener;

        private static int sleepListenerMillis;

        public EsperCEPProvider() {
        }

        public void init(final int _sleepListenerMillis) {
            sleepListenerMillis = _sleepListenerMillis;
            Configuration configuration;

            // EsperHA enablement - if available
            try {
                Class configurationHAClass = Class.forName("com.espertech.esperha.client.ConfigurationHA");
                configuration = (Configuration) configurationHAClass.newInstance();
                System.out.println("=== EsperHA is available, using ConfigurationHA ===");
            } catch (ClassNotFoundException e) {
                configuration = new Configuration();
            } catch (Throwable t) {
                System.err.println("Could not properly determine if EsperHA is available, default to Esper");
                t.printStackTrace();
                configuration = new Configuration();
            }
            configuration.addEventType("Market", MarketData.class);


            // EsperJMX enablement - if available
			try {
				Class.forName("com.espertech.esper.jmx.client.EsperJMXPlugin");
	            configuration.addPluginLoader(
	                    "EsperJMX",
	                    "com.espertech.esper.jmx.client.EsperJMXPlugin",
	    				null);// will use platform mbean - should enable platform mbean connector in startup command line
                System.out.println("=== EsperJMX is available, using platform mbean ===");
			} catch (ClassNotFoundException e) {
				;
			}


			
            //EPServiceProvider epService = EPServiceProviderManager.getProvider("benchmark", configuration);
            //esper = new EsperAdapter();
            Properties props = null;
            PropertyConfigurator.configure(
            		EsperCEPProvider.class.getClassLoader().getResource("config/esper.log4j.properties"));
    		try {
				props = ParameterUtils.load(
						EsperCEPProvider.class.getClassLoader().getResourceAsStream(
								"config/config_memoryStore.esper.properties"));
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
            props.put("configuration", configuration);
            /*
            try {
				esper.init(props);
			} catch (StreamAdapterException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
			
			try {
				si = new SemanticIntegrator(props);
			} catch (IntegratorRegistryException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IntegratorConfigurationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			esper = (EsperAdapter)si.getExecutor().getAdapter();
			
			
            listener = new EsperListener();
            //epAdministrator = epService.getEPAdministrator();
            //updateListener = new MyUpdateListener();
            //subscriber = new MySubscriber();
            //epRuntime = epService.getEPRuntime();
        }

        public void queryFactory()
        {
        	String queryTemp=loadQuery("queries/queryTemp.sparql");
        	try {
				PullDataSourceMetadata key = si.pullQueryFactory("urn:oeg:EsperTest", new QueryDocument(queryTemp));
			} catch (DataSourceException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (QueryException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        
        public void registerStatement(String statement, String statementID) {
        	String queryTemp=loadQuery("queries/queryTemp.sparql");
        	Statement sta = null;
			try {
				sta  = si.registerQuery("urn:oeg:EsperTest", new QueryDocument(queryTemp));
			} catch (QueryCompilerException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (DataSourceException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (QueryException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	EsperStatement esta = (EsperStatement)sta;//esper.registerQuery(statement);
        	esta.addListener(listener);
            //EPStatement stmt = epAdministrator.createEPL(statement, statementID);
        	/*
            if (System.getProperty("esper.benchmark.ul") != null) {
                stmt.addListener(updateListener);
            } else {
                stmt.setSubscriber(subscriber);
            }*/
        }

        public void sendEvent(Object event) {
        	esper.sendEvent(event);
            //epRuntime.sendEvent(event);
        }
    }

    public static class MyUpdateListener implements UpdateListener {
        public void update(EventBean[] newEvents, EventBean[] oldEvents) {
            if (newEvents != null) {
                if (EsperCEPProvider.sleepListenerMillis > 0) {
                    try {
                        Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                    } catch (InterruptedException ie) {
                        ;
                    }
                }
            }
        }
    }

    public static class MySubscriber {
        public void update(String ticker) {
            if (EsperCEPProvider.sleepListenerMillis > 0) {
                try {
                    Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                } catch (InterruptedException ie) {
                    ;
                }
            }
        }

        public void update(MarketData marketData) {
            if (EsperCEPProvider.sleepListenerMillis > 0) {
                try {
                    Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                } catch (InterruptedException ie) {
                    ;
                }
            }
        }

        public void update(String ticker, double avg, long count, double sum) {
            if (EsperCEPProvider.sleepListenerMillis > 0) {
                try {
                    Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                } catch (InterruptedException ie) {
                    ;
                }
            }
        }
        public void update(String ticker, double price, int vloume) {
        	try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	System.out.println(ticker+"-"+price+"-"+vloume);
            if (EsperCEPProvider.sleepListenerMillis > 0) {
                try {
                	
                    Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                } catch (InterruptedException ie) {
                    ;
                }
            }
        }
    }

}
