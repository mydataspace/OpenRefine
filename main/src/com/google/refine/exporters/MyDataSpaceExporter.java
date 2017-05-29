/*

Copyright 2010, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,           
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY           
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

package com.google.refine.exporters;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVWriter;

import com.google.refine.browsing.Engine;
import com.google.refine.model.Project;
import com.google.refine.util.JSONUtilities;
import com.google.refine.util.ParsingUtilities;

public class MyDataSpaceExporter implements WriterExporter{

    final static Logger logger = LoggerFactory.getLogger("CsvExporter");
    char separator;

    public MyDataSpaceExporter() {
        separator = ','; //Comma separated-value is default
    }

    @Override
    public void export(Project project, Properties params, Engine engine, final Writer writer)
            throws IOException {
        
        String optionsString = (params == null) ? null : params.getProperty("options");
        JSONObject options = null;
        if (optionsString != null) {
            try {
                options = ParsingUtilities.evaluateJsonStringToObject(optionsString);
            } catch (JSONException e) {
                // Ignore and keep options null.
            }
        }
        
        final String separator = options == null ? Character.toString(this.separator) :
            JSONUtilities.getString(options, "separator", Character.toString(this.separator));
        final String lineSeparator = options == null ? CSVWriter.DEFAULT_LINE_END :
            JSONUtilities.getString(options, "lineSeparator", CSVWriter.DEFAULT_LINE_END);
        final boolean quoteAll = options == null ? false : JSONUtilities.getBoolean(options, "quoteAll", false);
        
        final boolean printColumnHeader =
            (params != null && params.getProperty("printColumnHeader") != null) ?
                Boolean.parseBoolean(params.getProperty("printColumnHeader")) :
                true;

        final String fileName = UUID.randomUUID().toString();
        
        final FileWriter fileWriter = new FileWriter("/tmp/" + fileName + ".csv");
        
        final CSVWriter csvWriter = 
            new CSVWriter(fileWriter, separator.charAt(0), CSVWriter.DEFAULT_QUOTE_CHARACTER, lineSeparator);
        
        
        class Serializer implements TabularSerializer {
            int linesWritten = 0;
            
            @Override
            public void startFile(JSONObject options) {
            }

            @Override
            public void endFile() {
            }

            @Override
            public void addRow(List<CellData> cells, boolean isHeader) {
                if (!isHeader || printColumnHeader) {
                    String[] strings = new String[cells.size()];
                    for (int i = 0; i < strings.length; i++) {
                        CellData cellData = cells.get(i);
                        strings[i] =
                            (cellData != null && cellData.text != null) ?
                            cellData.text :
                            "";
                    }
                    csvWriter.writeNext(strings, quoteAll);
                    linesWritten++;
                }
            }
        };
        
        final Serializer serializer = new Serializer();
        
        CustomizableTabularExporterUtilities.exportRows(project, engine, params, serializer);
        
        writer.write("<script>window.parent.postMessage({ message: 'openRefineImport', id: '" + fileName + "', stage: 'created' }, '*')</script>");
        
        try {
            final JSONObject stats = new JSONObject();
            stats.put("lines", serializer.linesWritten);
            try (final FileWriter file = new FileWriter("/tmp/" + fileName + "-stats.json")) {
                file.write(stats.toString());
            }
        } catch (JSONException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
       
        csvWriter.close();
    }

    @Override
    public String getContentType() {
        return "text/html";
    }
}
