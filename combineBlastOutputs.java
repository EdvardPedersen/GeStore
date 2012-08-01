package org.diffdb;

import java.io.IOException; 
import java.util.*; 

import org.xml.sax.*;
import org.xml.sax.helpers.*;
import javax.xml.parsers.*;
 
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
 
public class combineBlastOutputs{
    private static Integer currentIter = 0;
    private static Boolean oldDone = false;
    private static String iteration_query;
    public static void main(String[] args) throws Exception { 
        String file1 = args[0];
        String file2 = args[1];
        String deleted = args[2];
        
        boolean wait_for_old_file = false;
        newFileParser NFP = new newFileParser(file1, deleted);
        Thread NFP_thread = new Thread(NFP);
        oldFileParser OFP = new oldFileParser(file2, NFP_thread);
        Thread OFP_thread = new Thread(OFP);
        NFP.set_OFP(OFP_thread);
        
        NFP_thread.start();
        OFP_thread.start();
        OFP_thread.join();
        NFP_thread.join();
        
    }
    
    
    private static class newFileParser implements Runnable{
        String filename;
        String deletedIds;
        Thread OFP_thread;
        
        public newFileParser(String file, String deleted) {
            filename = file;
            deletedIds = deleted;
        }
        
        public void set_OFP(Thread OFP) {
            OFP_thread = OFP;
        }
        
        public void run() {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            try {
                SAXParser saxParser = factory.newSAXParser();
                DefaultHandler slaveHandler = new DefaultHandler (){
                    int iteration_num = 0;
                    int hit_num = 0;
                    int hsp_num = 0;
                    int indentation_level = 0;
                    
                    String attribute_name = "";
                    String output_line = "";
                    
                    boolean startedElement = false;
                    boolean endedElement = false;
                    boolean writing = false;
                    
                    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
                        /*System.out.println("START uri = " + uri);
                        System.out.println("START localName = " + localName);
                        System.out.println("START qName = " + qName);*/
                        endedElement = false;
                        if(startedElement && writing) {
                            System.out.println(output_line);
                        }
                        output_line = "";
                        
                        for(int i = 0; i < indentation_level; i++) {
                            output_line = output_line + "  ";
                        }
                        
                        output_line = output_line + "<" + qName + ">";
                        
                        attribute_name = qName;
                        
                        if(qName.equals("Hit")) {
                            hit_num += 1;
                            hsp_num = 0;
                            writing = true;
                        } else if(qName.equals("Iteration_iter-num")) {
                            iteration_num += 1;
                            hsp_num = 0;
                            hit_num = 0;
                            if(iteration_num == currentIter) {
                                try {
                                    while(true) {
                                        Thread.sleep(1000);
                                    }
                                } catch (InterruptedException e) {
                                
                                }
                            }
                        } else if(qName.equals("Hsp")) {
                            hsp_num += 1;
                        }
                        indentation_level += 1;
                        startedElement = true;
                    }
                
                    public void characters(char ch[], int start, int length) throws SAXException {
                        
                        if(attribute_name.equals("Hit_id")) {
                            output_line = output_line + new String(ch, start, length);
                        } else {
                            output_line = output_line + new String(ch, start, length);
                        }
                    }
                    
                    public void endElement(String uti, String localName, String qName) throws SAXException {
                        startedElement = false;
                        
                        indentation_level -= 1;
                        
                        if(endedElement) {
                            output_line = "";
                            for(int i = 0; i < indentation_level; i++) {
                                output_line = output_line + "  ";
                            }
                        }
                        
                        output_line = output_line + "</" + qName + ">";
                        
                        if(qName.equals("Hit")) {
                            writing = false;
                            System.out.println(output_line);
                        } else if(qName.equals("Iteration")) {
                            OFP_thread.interrupt();
                        }
                        if(oldDone) {
                            writing = true;
                        }
                        
                        if(writing || oldDone) {
                            System.out.println(output_line);
                        }
                        endedElement = true;
                    }
                };
                saxParser.parse(filename, slaveHandler);
            } catch (Exception E) {
                return;
            }
        }
    }
    
    private static class oldFileParser implements Runnable {
        String filename;
        Thread new_file_parser;
        
        public oldFileParser(String file, Thread newFP) {
            filename = file;
            new_file_parser = newFP;
        }
        
        public void run() {
            try {
                SAXParserFactory factory = SAXParserFactory.newInstance();
                SAXParser saxParser = factory.newSAXParser();
                DefaultHandler handler = new DefaultHandler() {
                    int iteration_num = 0;
                    int hit_num = 0;
                    int hsp_num = 0;
                    int indentation_level = 0;
                    
                    String attribute_name = "";
                    String output_line = "";
                    
                    boolean startedElement = false;
                    boolean endedElement = false;
                    
                    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
                        /*System.out.println("START uri = " + uri);
                        System.out.println("START localName = " + localName);
                        System.out.println("START qName = " + qName);*/
                        endedElement = false;
                        if(startedElement) {
                            System.out.println(output_line);
                        }
                        output_line = "";
                        
                        for(int i = 0; i < indentation_level; i++) {
                            output_line = output_line + "  ";
                        }
                        
                        output_line = output_line + "<" + qName + ">";
                        
                        attribute_name = qName;
                        
                        if(qName.equals("Hit_id")) {
                            hit_num += 1;
                            hsp_num = 0;
                        } else if(qName.equals("Iteration")) {
                            iteration_num += 1;
                            currentIter += 1;
                            hsp_num = 0;
                            hit_num = 0;
                        } else if(qName.equals("Hsp")) {
                            hsp_num += 1;
                        }
                        indentation_level += 1;
                        startedElement = true;
                    }
                
                    public void characters(char ch[], int start, int length) throws SAXException {
                        
                        if(attribute_name.equals("Hit_id")) {
                            output_line = output_line + new String(ch, start, length);
                        } else {
                            output_line = output_line + new String(ch, start, length);
                        }
                    }
                    
                    public void endElement(String uti, String localName, String qName) throws SAXException {
                        startedElement = false;
                        
                        indentation_level -= 1;
                        
                        if(endedElement) {
                            output_line = "";
                            for(int i = 0; i < indentation_level; i++) {
                                output_line = output_line + "  ";
                            }
                        }
                        output_line = output_line + "</" + qName + ">";
                        endedElement = true;
                        
                        if(qName.equals("Iteration_hits")) {
                            new_file_parser.interrupt();
                            try {
                                while(true) {
                                    Thread.sleep(1000);
                                }
                            } catch (InterruptedException E) {
                                
                            }
                        } else if(qName.equals("BlastOutput_iterations")) {
                            oldDone = true;
                            new_file_parser.interrupt();
                            return;
                        }
                        System.out.println(output_line);
                    }
                    
                    public void processingInstruction(String target, String data) throws SAXException {
                        System.out.println(target + " " + data);
                    }
                };
                saxParser.parse(filename, handler);
            } catch (Exception E) {
                return;
            }
        }
    }
}