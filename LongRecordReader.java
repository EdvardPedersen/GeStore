/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Based on LineRecordReader from the Hadoop 0.20 distrobution
 */

package org.diffdb;

import java.io.IOException;
import java.util.regex.*; 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.nio.ByteBuffer;

/**
 * Read arbitrary records from file.
 * Based on LineRecordReader
 * Returns only complete lines
 */
public class LongRecordReader extends LineRecordReader{
    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private int maxLineLength;
    private LongWritable key = null;
    private Text value = null;
    private byte[] recordDelimiterBytes;
    
    private Pattern startRecordRegex = Pattern.compile(".*");
    private Pattern endRecordRegex = Pattern.compile(".*");
    
    public LongRecordReader (String beginRegex, String endRegex) {
        startRecordRegex = Pattern.compile(beginRegex);
        endRecordRegex = Pattern.compile(endRegex);
    }
    
    @Override
    public boolean nextKeyValue() throws IOException {
        if (key == null) {
            key = new LongWritable();
        }
        key.set(pos);
        value = new Text();
        int newSize = 0;
        if (pos < end) {
            newSize = getEntry(startRecordRegex, endRecordRegex);
            pos += newSize;
        }
        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
    }
    
    // Copied from LineRecordReader
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = compressionCodecs.getCodec(file);

        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());
        boolean skipFirstLine = false;
        if (codec != null) {
            if (null == this.recordDelimiterBytes) {
                in = new LineReader(codec.createInputStream(fileIn), job);
            } else {
                in = new LineReader(codec.createInputStream(fileIn), job);
            }
            end = Long.MAX_VALUE;
        } else {
            if (start != 0) {
                skipFirstLine = true;
                --start;
                fileIn.seek(start);
            }
            if (null == this.recordDelimiterBytes) {
                in = new LineReader(fileIn, job);
            } else {
                in = new LineReader(fileIn, job);
            }
        }
        this.pos = start;
    }
    
    @Override
    public LongWritable getCurrentKey() {
        return key;
    }
    
    @Override
    public Text getCurrentValue() {
        return value;
    }
    
    /******
     * Gets one complete entry
     */
    
    private int getEntry(Pattern matcherStart, Pattern matcherStop) throws IOException {
        boolean started = false;
        boolean done = false;
        
        ByteBuffer newLine = ByteBuffer.allocate(2);
        newLine.putChar('\n');
        byte[] newLineBytes = newLine.array();
        
        Text tempLine  = new Text();
        int totalRead = 0;
        int newRead = 0;
        // Discard lines before start record match, save first line that matches regex
        while(!started){
            newRead = in.readLine(tempLine, maxLineLength, Math.max((int)Math.min(Integer.MAX_VALUE, end-pos), maxLineLength));
            if(newRead == 0) {
                return 0;
            }
            totalRead += newRead;
            Matcher m = matcherStart.matcher(tempLine.toString());
            if(m.matches()){
                started = true;
                tempLine.append(newLineBytes, 0, newLineBytes.length);
                value.append(tempLine.getBytes(), 0, tempLine.getLength());
                break;
            }
        }
        
        // Save lines until end record match, save last line
        while(!done){
            newRead = in.readLine(tempLine, maxLineLength, Math.max((int)Math.min(Integer.MAX_VALUE, end-pos), maxLineLength));
            if(newRead == 0){
                return 0;
            }
            totalRead += newRead;
            Matcher m = matcherStop.matcher(tempLine.toString());
            if(m.matches()){
                done = true;
            }
            tempLine.append(newLineBytes, 0, newLineBytes.length);
            value.append(tempLine.getBytes(), 0, tempLine.getLength());
        }
        return totalRead;
    }
}