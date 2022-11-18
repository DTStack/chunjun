/*
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
 */

package com.dtstack.chunjun.connector.s3.util;

import com.google.common.collect.Maps;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.NumberFormat;
import java.util.HashMap;

/** A stream based parser for parsing delimited text data from a file or a stream. */
public class ReaderUtil {

    private long nextOffset;

    private Reader inputStream = null;

    private String fileName = null;

    // this holds all the values for switches that the user is allowed to set
    private final UserSettings userSettings = new UserSettings();

    private Charset charset = null;

    private boolean useCustomRecordDelimiter = false;

    // this will be our working buffer to hold data chunks
    // read in from the data file

    private final DataBuffer dataBuffer = new DataBuffer();

    private final ColumnBuffer columnBuffer = new ColumnBuffer();

    // 缓存当前记录的值，当一个条记录需要跨两个流缓存读取时，需要将已读取的部分记录的字符缓存至此
    private final RawRecordBuffer rawBuffer = new RawRecordBuffer();

    private boolean[] isQualified = null;

    private String rawRecord = "";

    private final HeadersHolder headersHolder = new HeadersHolder();

    // these are all more or less global loop variables
    // to keep from needing to pass them all into various
    // methods during parsing

    private boolean startedColumn = false;

    private boolean startedWithQualifier = false;

    private boolean hasMoreData = true;

    private char lastLetter = '\0';

    private boolean hasReadNextLine = false;

    private int columnsCount = 0;

    private long currentRecord = 0;

    private String[] values = new String[StaticSettings.INITIAL_COLUMN_COUNT];

    private boolean initialized = false;

    private boolean closed = false;

    /** Double up the text qualifier to represent an occurance of the text qualifier. */
    public static final int ESCAPE_MODE_DOUBLED = 1;

    /**
     * Use a backslash character before the text qualifier to represent an occurance of the text
     * qualifier.
     */
    public static final int ESCAPE_MODE_BACKSLASH = 2;

    public ReaderUtil(String fileName, char delimiter, Charset charset)
            throws FileNotFoundException {
        this(fileName, delimiter, charset, 0L);
    }

    public ReaderUtil(String fileName, char delimiter, Charset charset, Long nextOffset)
            throws FileNotFoundException {
        if (fileName == null) {
            throw new IllegalArgumentException("Parameter fileName can not be null.");
        }

        if (charset == null) {
            throw new IllegalArgumentException("Parameter charset can not be null.");
        }

        if (!new File(fileName).exists()) {
            throw new FileNotFoundException("File " + fileName + " does not exist.");
        }

        this.fileName = fileName;
        this.userSettings.Delimiter = delimiter;
        this.charset = charset;
        this.nextOffset = nextOffset;

        isQualified = new boolean[values.length];
    }

    public ReaderUtil(String fileName, char delimiter) throws FileNotFoundException {
        this(fileName, delimiter, StandardCharsets.ISO_8859_1);
    }

    public ReaderUtil(String fileName) throws FileNotFoundException {
        this(fileName, Letters.COMMA);
    }

    public ReaderUtil(Reader inputStream, char delimiter) {
        this(inputStream, delimiter, 0L, false);
    }

    public ReaderUtil(Reader inputStream, char delimiter, Long nextOffset, boolean safetySwitch) {
        if (inputStream == null) {
            throw new IllegalArgumentException("Parameter inputStream can not be null.");
        }

        this.inputStream = inputStream;
        this.userSettings.Delimiter = delimiter;
        this.userSettings.SafetySwitch = safetySwitch;
        initialized = true;
        this.nextOffset = nextOffset;

        isQualified = new boolean[values.length];
    }

    public ReaderUtil(Reader inputStream) {
        this(inputStream, Letters.COMMA);
    }

    public ReaderUtil(InputStream inputStream, char delimiter, Charset charset, Long nextOffset) {
        this(new InputStreamReader(inputStream, charset), delimiter, nextOffset, false);
    }

    public ReaderUtil(InputStream inputStream, char delimiter, Charset charset) {
        this(new InputStreamReader(inputStream, charset), delimiter, 0L, false);
    }

    public ReaderUtil(InputStream inputStream, Charset charset) {
        this(new InputStreamReader(inputStream, charset));
    }

    public boolean getCaptureRawRecord() {
        return userSettings.CaptureRawRecord;
    }

    public void setCaptureRawRecord(boolean captureRawRecord) {
        userSettings.CaptureRawRecord = captureRawRecord;
    }

    public String getRawRecord() {
        return rawRecord;
    }

    /**
     * Gets whether leading and trailing whitespace characters are being trimmed from
     * non-textqualified column data. Default is true.
     *
     * @return Whether leading and trailing whitespace characters are being trimmed from
     *     non-textqualified column data.
     */
    public boolean getTrimWhitespace() {
        return userSettings.TrimWhitespace;
    }

    /**
     * Sets whether leading and trailing whitespace characters should be trimmed from
     * non-textqualified column data or not. Default is true.
     *
     * @param trimWhitespace Whether leading and trailing whitespace characters should be trimmed
     *     from non-textqualified column data or not.
     */
    public void setTrimWhitespace(boolean trimWhitespace) {
        userSettings.TrimWhitespace = trimWhitespace;
    }

    /**
     * Gets the character being used as the column delimiter. Default is comma, ','.
     *
     * @return The character being used as the column delimiter.
     */
    public char getDelimiter() {
        return userSettings.Delimiter;
    }

    /**
     * Sets the character to use as the column delimiter. Default is comma, ','.
     *
     * @param delimiter The character to use as the column delimiter.
     */
    public void setDelimiter(char delimiter) {
        userSettings.Delimiter = delimiter;
    }

    public char getRecordDelimiter() {
        return userSettings.RecordDelimiter;
    }

    /**
     * Sets the character to use as the record delimiter.
     *
     * @param recordDelimiter The character to use as the record delimiter. Default is combination
     *     of standard end of line characters for Windows, Unix, or Mac.
     */
    public void setRecordDelimiter(char recordDelimiter) {
        useCustomRecordDelimiter = true;
        userSettings.RecordDelimiter = recordDelimiter;
    }

    /**
     * Gets the character to use as a text qualifier in the data.
     *
     * @return The character to use as a text qualifier in the data.
     */
    public char getTextQualifier() {
        return userSettings.TextQualifier;
    }

    /**
     * Sets the character to use as a text qualifier in the data.
     *
     * @param textQualifier The character to use as a text qualifier in the data.
     */
    public void setTextQualifier(char textQualifier) {
        userSettings.TextQualifier = textQualifier;
    }

    /**
     * Whether text qualifiers will be used while parsing or not.
     *
     * @return Whether text qualifiers will be used while parsing or not.
     */
    public boolean getUseTextQualifier() {
        return userSettings.UseTextQualifier;
    }

    /**
     * Sets whether text qualifiers will be used while parsing or not.
     *
     * @param useTextQualifier Whether to use a text qualifier while parsing or not.
     */
    public void setUseTextQualifier(boolean useTextQualifier) {
        userSettings.UseTextQualifier = useTextQualifier;
    }

    /**
     * Gets the character being used as a comment signal.
     *
     * @return The character being used as a comment signal.
     */
    public char getComment() {
        return userSettings.Comment;
    }

    /**
     * Sets the character to use as a comment signal.
     *
     * @param comment The character to use as a comment signal.
     */
    public void setComment(char comment) {
        userSettings.Comment = comment;
    }

    /**
     * Gets whether comments are being looked for while parsing or not.
     *
     * @return Whether comments are being looked for while parsing or not.
     */
    public boolean getUseComments() {
        return userSettings.UseComments;
    }

    /**
     * Sets whether comments are being looked for while parsing or not.
     *
     * @param useComments Whether comments are being looked for while parsing or not.
     */
    public void setUseComments(boolean useComments) {
        userSettings.UseComments = useComments;
    }

    /**
     * Gets the current way to escape an occurance of the text qualifier inside qualified data.
     *
     * @return The current way to escape an occurance of the text qualifier inside qualified data.
     */
    public int getEscapeMode() {
        return userSettings.EscapeMode;
    }

    /**
     * Sets the current way to escape an occurance of the text qualifier inside qualified data.
     *
     * @param escapeMode The way to escape an occurance of the text qualifier inside qualified data.
     * @throws IllegalArgumentException When an illegal value is specified for escapeMode.
     */
    public void setEscapeMode(int escapeMode) throws IllegalArgumentException {
        if (escapeMode != ESCAPE_MODE_DOUBLED && escapeMode != ESCAPE_MODE_BACKSLASH) {
            throw new IllegalArgumentException("Parameter escapeMode must be a valid value.");
        }

        userSettings.EscapeMode = escapeMode;
    }

    public boolean getSkipEmptyRecords() {
        return userSettings.SkipEmptyRecords;
    }

    public void setSkipEmptyRecords(boolean skipEmptyRecords) {
        userSettings.SkipEmptyRecords = skipEmptyRecords;
    }

    /**
     * Safety caution to prevent the parser from using large amounts of memory in the case where
     * parsing settings like file encodings don't end up matching the actual format of a file. This
     * switch can be turned off if the file format is known and tested. With the switch off, the max
     * column lengths and max column count per record supported by the parser will greatly increase.
     * Default is true.
     *
     * @return The current setting of the safety switch.
     */
    public boolean getSafetySwitch() {
        return userSettings.SafetySwitch;
    }

    /**
     * Safety caution to prevent the parser from using large amounts of memory in the case where
     * parsing settings like file encodings don't end up matching the actual format of a file. This
     * switch can be turned off if the file format is known and tested. With the switch off, the max
     * column lengths and max column count per record supported by the parser will greatly increase.
     * Default is true.
     *
     * @param safetySwitch
     */
    public void setSafetySwitch(boolean safetySwitch) {
        userSettings.SafetySwitch = safetySwitch;
    }

    /**
     * Gets the count of columns found in this record.
     *
     * @return The count of columns found in this record.
     */
    public int getColumnCount() {
        return columnsCount;
    }

    /**
     * Gets the index of the current record.
     *
     * @return The index of the current record.
     */
    public long getCurrentRecord() {
        return currentRecord - 1;
    }

    public int getHeaderCount() {
        return headersHolder.Length;
    }

    /**
     * Returns the header values as a string array.
     *
     * @return The header values as a String array.
     * @throws IOException Thrown if this object has already been closed.
     */
    public String[] getHeaders() throws IOException {
        checkClosed();

        if (headersHolder.Headers == null) {
            return null;
        } else {
            // use clone here to prevent the outside code from
            // setting values on the array directly, which would
            // throw off the index lookup based on header name
            String[] clone = new String[headersHolder.Length];
            System.arraycopy(headersHolder.Headers, 0, clone, 0, headersHolder.Length);
            return clone;
        }
    }

    public void setHeaders(String[] headers) {
        headersHolder.Headers = headers;

        headersHolder.IndexByName.clear();

        if (headers != null) {
            headersHolder.Length = headers.length;
        } else {
            headersHolder.Length = 0;
        }

        // use headersHolder.Length here in case headers is null
        for (int i = 0; i < headersHolder.Length; i++) {
            headersHolder.IndexByName.put(headers[i], new Integer(i));
        }
    }

    public String[] getValues() throws IOException {
        checkClosed();

        // need to return a clone, and can't use clone because values.Length
        // might be greater than columnsCount
        String[] clone = new String[columnsCount];
        System.arraycopy(values, 0, clone, 0, columnsCount);
        return clone;
    }

    /**
     * Returns the current column value for a given column index.
     *
     * @param columnIndex The index of the column.
     * @return The current column value.
     * @throws IOException Thrown if this object has already been closed.
     */
    public String get(int columnIndex) throws IOException {
        checkClosed();

        if (columnIndex > -1 && columnIndex < columnsCount) {
            return values[columnIndex];
        } else {
            return "";
        }
    }

    /**
     * Returns the current column value for a given column header name.
     *
     * @param headerName The header name of the column.
     * @return The current column value.
     * @throws IOException Thrown if this object has already been closed.
     */
    public String get(String headerName) throws IOException {
        checkClosed();

        return get(getIndex(headerName));
    }

    public static ReaderUtil parse(String data) {
        if (data == null) {
            throw new IllegalArgumentException("Parameter data can not be null.");
        }

        return new ReaderUtil(new StringReader(data));
    }

    /**
     * Reads another record.
     *
     * @return Whether another record was successfully read or not.
     * @throws IOException Thrown if an error occurs while reading data from the source stream.
     */
    public boolean readRecord() throws IOException {
        checkClosed();

        columnsCount = 0;
        rawBuffer.Position = 0;

        dataBuffer.LineStart = dataBuffer.Position;

        hasReadNextLine = false;

        // 确认下是否还存在数据

        if (hasMoreData) {
            // 遍历数据流，直到找到数据的末尾或找到记录的末尾

            do {
                if (dataBuffer.Position == dataBuffer.Count) {
                    // 在遍历过程中如果遍历完数据，且还没找到这一行的结束符，则填充数据
                    checkDataLength();
                } else {
                    startedWithQualifier = false;

                    // 抓取当前位置字母作为字符

                    char currentLetter = dataBuffer.Buffer[dataBuffer.Position];

                    if (userSettings.UseTextQualifier
                            && currentLetter == userSettings.TextQualifier) {
                        // 这将是一个文本限定列，因此我们需要设置startsWithQualifier使其进入单独的分支以处理文本限定列

                        lastLetter = currentLetter;

                        // read qualified
                        startedColumn = true;
                        // 读到文本限定符意味下一个字节是这个字段的起始位置
                        dataBuffer.ColumnStart = dataBuffer.Position + 1;
                        // 标记一下这个字段是以文本限定符开始的
                        startedWithQualifier = true;
                        boolean lastLetterWasQualifier = false;

                        char escapeChar = userSettings.TextQualifier;

                        if (userSettings.EscapeMode == ESCAPE_MODE_BACKSLASH) {
                            escapeChar = Letters.BACKSLASH;
                        }

                        // 在一个字段中遇到文本限定符
                        boolean eatingTrailingJunk = false;
                        boolean lastLetterWasEscape = false;
                        boolean readingComplexEscape = false;
                        int escape = ComplexEscape.UNICODE;
                        int escapeLength = 0;
                        char escapeValue = (char) 0;

                        // 偏移量+1，即移动到字段开始的偏移位置
                        dataBuffer.Position++;

                        // 从起始文本限定符开始循环遍历dataBuffer的数据，获取当前字段的值
                        do {
                            if (dataBuffer.Position == dataBuffer.Count) {
                                // 如果在之前偏移量移动之后到了缓存的数据的最后，则填充数据
                                checkDataLength();
                            } else {
                                // 抓取当前字母作为字符

                                currentLetter = dataBuffer.Buffer[dataBuffer.Position];

                                if (eatingTrailingJunk) {
                                    dataBuffer.ColumnStart = dataBuffer.Position + 1;

                                    if (currentLetter == userSettings.Delimiter) {
                                        endColumn();
                                    } else if ((!useCustomRecordDelimiter
                                                    && (currentLetter == Letters.CR
                                                            || currentLetter == Letters.LF))
                                            || (useCustomRecordDelimiter
                                                    && currentLetter
                                                            == userSettings.RecordDelimiter)) {
                                        endColumn();

                                        endRecord();
                                    }
                                } else if (readingComplexEscape) {
                                    escapeLength++;

                                    switch (escape) {
                                        case ComplexEscape.UNICODE:
                                            escapeValue *= (char) 16;
                                            escapeValue += hexToDec(currentLetter);

                                            if (escapeLength == 4) {
                                                readingComplexEscape = false;
                                            }

                                            break;
                                        case ComplexEscape.OCTAL:
                                            escapeValue *= (char) 8;
                                            escapeValue += (char) (currentLetter - '0');

                                            if (escapeLength == 3) {
                                                readingComplexEscape = false;
                                            }

                                            break;
                                        case ComplexEscape.DECIMAL:
                                            escapeValue *= (char) 10;
                                            escapeValue += (char) (currentLetter - '0');

                                            if (escapeLength == 3) {
                                                readingComplexEscape = false;
                                            }

                                            break;
                                        case ComplexEscape.HEX:
                                            escapeValue *= (char) 16;
                                            escapeValue += hexToDec(currentLetter);

                                            if (escapeLength == 2) {
                                                readingComplexEscape = false;
                                            }

                                            break;
                                    }

                                    if (!readingComplexEscape) {
                                        appendLetter(escapeValue);
                                    } else {
                                        dataBuffer.ColumnStart = dataBuffer.Position + 1;
                                    }
                                } else if (currentLetter == userSettings.TextQualifier) {
                                    // 如果当前读取到的字符为文本限定符
                                    if (lastLetterWasEscape) {
                                        lastLetterWasEscape = false;
                                        lastLetterWasQualifier = false;
                                    } else {
                                        updateCurrentValue();

                                        if (userSettings.EscapeMode == ESCAPE_MODE_DOUBLED) {
                                            lastLetterWasEscape = true;
                                        }

                                        lastLetterWasQualifier = true;
                                    }
                                } else if (userSettings.EscapeMode == ESCAPE_MODE_BACKSLASH
                                        && lastLetterWasEscape) {
                                    switch (currentLetter) {
                                        case 'n':
                                            appendLetter(Letters.LF);
                                            break;
                                        case 'r':
                                            appendLetter(Letters.CR);
                                            break;
                                        case 't':
                                            appendLetter(Letters.TAB);
                                            break;
                                        case 'b':
                                            appendLetter(Letters.BACKSPACE);
                                            break;
                                        case 'f':
                                            appendLetter(Letters.FORM_FEED);
                                            break;
                                        case 'e':
                                            appendLetter(Letters.ESCAPE);
                                            break;
                                        case 'v':
                                            appendLetter(Letters.VERTICAL_TAB);
                                            break;
                                        case 'a':
                                            appendLetter(Letters.ALERT);
                                            break;
                                        case '0':
                                        case '1':
                                        case '2':
                                        case '3':
                                        case '4':
                                        case '5':
                                        case '6':
                                        case '7':
                                            escape = ComplexEscape.OCTAL;
                                            readingComplexEscape = true;
                                            escapeLength = 1;
                                            escapeValue = (char) (currentLetter - '0');
                                            dataBuffer.ColumnStart = dataBuffer.Position + 1;
                                            break;
                                        case 'u':
                                        case 'x':
                                        case 'o':
                                        case 'd':
                                        case 'U':
                                        case 'X':
                                        case 'O':
                                        case 'D':
                                            switch (currentLetter) {
                                                case 'u':
                                                case 'U':
                                                    escape = ComplexEscape.UNICODE;
                                                    break;
                                                case 'x':
                                                case 'X':
                                                    escape = ComplexEscape.HEX;
                                                    break;
                                                case 'o':
                                                case 'O':
                                                    escape = ComplexEscape.OCTAL;
                                                    break;
                                                case 'd':
                                                case 'D':
                                                    escape = ComplexEscape.DECIMAL;
                                                    break;
                                            }

                                            readingComplexEscape = true;
                                            escapeLength = 0;
                                            escapeValue = (char) 0;
                                            dataBuffer.ColumnStart = dataBuffer.Position + 1;

                                            break;
                                        default:
                                            break;
                                    }

                                    lastLetterWasEscape = false;

                                    // can only happen for ESCAPE_MODE_BACKSLASH
                                } else if (currentLetter == escapeChar) {
                                    updateCurrentValue();
                                    lastLetterWasEscape = true;
                                } else {
                                    if (lastLetterWasQualifier) {
                                        // 若上一个字符是文本限定符
                                        if (currentLetter == userSettings.Delimiter) {
                                            // 且当前字符是字段分隔符，则认为该字段获取完
                                            endColumn();
                                        } else if ((!useCustomRecordDelimiter
                                                        && (currentLetter == Letters.CR
                                                                || currentLetter == Letters.LF))
                                                || (useCustomRecordDelimiter
                                                        && currentLetter
                                                                == userSettings.RecordDelimiter)) {
                                            // 且 当前字符是默认换行符或者是自定义换行符，则认为改记录获取完毕
                                            endColumn();

                                            endRecord();
                                        } else {
                                            // 且当前字符是其他字符，则认为该字段的值还未结束，
                                            // 上个文本限定符是字段内的一个普通字符，偏移量+1，eatingTrailingJunk 置 true
                                            dataBuffer.ColumnStart = dataBuffer.Position + 1;

                                            eatingTrailingJunk = true;
                                        }

                                        // make sure to clear the flag for next
                                        // run of the loop

                                        lastLetterWasQualifier = false;
                                    }
                                }

                                // 记录最后获得的字符，需要给下文使用
                                // it for several key decisions

                                lastLetter = currentLetter;
                                if (startedColumn) {
                                    // 如果已经开始读取这一个字段的数据了，偏移量 +1
                                    dataBuffer.Position++;

                                    if (userSettings.SafetySwitch
                                            && dataBuffer.Position
                                                            - dataBuffer.ColumnStart
                                                            + columnBuffer.Position
                                                    > 100000) {
                                        close();

                                        throw new IOException(
                                                "Maximum column length of 100,000 exceeded in column "
                                                        + NumberFormat.getIntegerInstance()
                                                                .format(columnsCount)
                                                        + " in record "
                                                        + NumberFormat.getIntegerInstance()
                                                                .format(currentRecord)
                                                        + ". Set the SafetySwitch property to false"
                                                        + " if you're expecting column lengths greater than 100,000 characters to"
                                                        + " avoid this error.");
                                    }
                                }
                            } // end else

                        } while (hasMoreData && startedColumn);
                    } else if (currentLetter == userSettings.Delimiter) {
                        // we encountered a column with no data, so
                        // just send the end column

                        lastLetter = currentLetter;

                        endColumn();
                    } else if (useCustomRecordDelimiter
                            && currentLetter == userSettings.RecordDelimiter) {
                        // this will skip blank lines
                        if (startedColumn || columnsCount > 0 || !userSettings.SkipEmptyRecords) {
                            endColumn();

                            endRecord();
                        } else {
                            dataBuffer.LineStart = dataBuffer.Position + 1;
                        }

                        lastLetter = currentLetter;
                    } else if (!useCustomRecordDelimiter
                            && (currentLetter == Letters.CR || currentLetter == Letters.LF)) {
                        // this will skip blank lines
                        if (startedColumn
                                || columnsCount > 0
                                || (!userSettings.SkipEmptyRecords
                                        && (currentLetter == Letters.CR
                                                || lastLetter != Letters.CR))) {
                            endColumn();

                            endRecord();
                        } else {
                            dataBuffer.LineStart = dataBuffer.Position + 1;
                        }

                        lastLetter = currentLetter;
                    } else if (userSettings.UseComments
                            && columnsCount == 0
                            && currentLetter == userSettings.Comment) {
                        // encountered a comment character at the beginning of
                        // the line so just ignore the rest of the line

                        lastLetter = currentLetter;

                        skipLine();
                    } else if (userSettings.TrimWhitespace
                            && (currentLetter == Letters.SPACE || currentLetter == Letters.TAB)) {
                        // do nothing, this will trim leading whitespace
                        // for both text qualified columns and non

                        startedColumn = true;
                        dataBuffer.ColumnStart = dataBuffer.Position + 1;
                    } else {
                        // since the letter wasn't a special letter, this
                        // will be the first letter of our current column

                        startedColumn = true;
                        dataBuffer.ColumnStart = dataBuffer.Position;
                        boolean lastLetterWasBackslash = false;
                        boolean readingComplexEscape = false;
                        int escape = ComplexEscape.UNICODE;
                        int escapeLength = 0;
                        char escapeValue = (char) 0;

                        boolean firstLoop = true;

                        do {
                            if (!firstLoop && dataBuffer.Position == dataBuffer.Count) {
                                checkDataLength();
                            } else {
                                if (!firstLoop) {
                                    // grab the current letter as a char
                                    currentLetter = dataBuffer.Buffer[dataBuffer.Position];
                                }

                                if (!userSettings.UseTextQualifier
                                        && userSettings.EscapeMode == ESCAPE_MODE_BACKSLASH
                                        && currentLetter == Letters.BACKSLASH) {
                                    if (lastLetterWasBackslash) {
                                        lastLetterWasBackslash = false;
                                    } else {
                                        updateCurrentValue();
                                        lastLetterWasBackslash = true;
                                    }
                                } else if (readingComplexEscape) {
                                    escapeLength++;

                                    switch (escape) {
                                        case ComplexEscape.UNICODE:
                                            escapeValue *= (char) 16;
                                            escapeValue += hexToDec(currentLetter);

                                            if (escapeLength == 4) {
                                                readingComplexEscape = false;
                                            }

                                            break;
                                        case ComplexEscape.OCTAL:
                                            escapeValue *= (char) 8;
                                            escapeValue += (char) (currentLetter - '0');

                                            if (escapeLength == 3) {
                                                readingComplexEscape = false;
                                            }

                                            break;
                                        case ComplexEscape.DECIMAL:
                                            escapeValue *= (char) 10;
                                            escapeValue += (char) (currentLetter - '0');

                                            if (escapeLength == 3) {
                                                readingComplexEscape = false;
                                            }

                                            break;
                                        case ComplexEscape.HEX:
                                            escapeValue *= (char) 16;
                                            escapeValue += hexToDec(currentLetter);

                                            if (escapeLength == 2) {
                                                readingComplexEscape = false;
                                            }

                                            break;
                                    }

                                    if (!readingComplexEscape) {
                                        appendLetter(escapeValue);
                                    } else {
                                        dataBuffer.ColumnStart = dataBuffer.Position + 1;
                                    }
                                } else if (userSettings.EscapeMode == ESCAPE_MODE_BACKSLASH
                                        && lastLetterWasBackslash) {
                                    switch (currentLetter) {
                                        case 'n':
                                            appendLetter(Letters.LF);
                                            break;
                                        case 'r':
                                            appendLetter(Letters.CR);
                                            break;
                                        case 't':
                                            appendLetter(Letters.TAB);
                                            break;
                                        case 'b':
                                            appendLetter(Letters.BACKSPACE);
                                            break;
                                        case 'f':
                                            appendLetter(Letters.FORM_FEED);
                                            break;
                                        case 'e':
                                            appendLetter(Letters.ESCAPE);
                                            break;
                                        case 'v':
                                            appendLetter(Letters.VERTICAL_TAB);
                                            break;
                                        case 'a':
                                            appendLetter(Letters.ALERT);
                                            break;
                                        case '0':
                                        case '1':
                                        case '2':
                                        case '3':
                                        case '4':
                                        case '5':
                                        case '6':
                                        case '7':
                                            escape = ComplexEscape.OCTAL;
                                            readingComplexEscape = true;
                                            escapeLength = 1;
                                            escapeValue = (char) (currentLetter - '0');
                                            dataBuffer.ColumnStart = dataBuffer.Position + 1;
                                            break;
                                        case 'u':
                                        case 'x':
                                        case 'o':
                                        case 'd':
                                        case 'U':
                                        case 'X':
                                        case 'O':
                                        case 'D':
                                            switch (currentLetter) {
                                                case 'u':
                                                case 'U':
                                                    escape = ComplexEscape.UNICODE;
                                                    break;
                                                case 'x':
                                                case 'X':
                                                    escape = ComplexEscape.HEX;
                                                    break;
                                                case 'o':
                                                case 'O':
                                                    escape = ComplexEscape.OCTAL;
                                                    break;
                                                case 'd':
                                                case 'D':
                                                    escape = ComplexEscape.DECIMAL;
                                                    break;
                                            }

                                            readingComplexEscape = true;
                                            escapeLength = 0;
                                            escapeValue = (char) 0;
                                            dataBuffer.ColumnStart = dataBuffer.Position + 1;

                                            break;
                                        default:
                                            break;
                                    }

                                    lastLetterWasBackslash = false;
                                } else {
                                    if (currentLetter == userSettings.Delimiter) {
                                        endColumn();
                                    } else if ((!useCustomRecordDelimiter
                                                    && (currentLetter == Letters.CR
                                                            || currentLetter == Letters.LF))
                                            || (useCustomRecordDelimiter
                                                    && currentLetter
                                                            == userSettings.RecordDelimiter)) {
                                        endColumn();

                                        endRecord();
                                    }
                                }

                                // keep track of the last letter because we need
                                // it for several key decisions

                                lastLetter = currentLetter;
                                firstLoop = false;

                                if (startedColumn) {
                                    dataBuffer.Position++;

                                    if (userSettings.SafetySwitch
                                            && dataBuffer.Position
                                                            - dataBuffer.ColumnStart
                                                            + columnBuffer.Position
                                                    > 100000) {
                                        close();

                                        throw new IOException(
                                                "Maximum column length of 100,000 exceeded in column "
                                                        + NumberFormat.getIntegerInstance()
                                                                .format(columnsCount)
                                                        + " in record "
                                                        + NumberFormat.getIntegerInstance()
                                                                .format(currentRecord)
                                                        + ". Set the SafetySwitch property to false"
                                                        + " if you're expecting column lengths greater than 100,000 characters to"
                                                        + " avoid this error.");
                                    }
                                }
                            } // end else
                        } while (hasMoreData && startedColumn);
                    }

                    if (hasMoreData) {
                        dataBuffer.Position++;
                    }
                } // end else
            } while (hasMoreData && !hasReadNextLine);

            // check to see if we hit the end of the file
            // without processing the current record

            if (startedColumn || lastLetter == userSettings.Delimiter) {
                endColumn();

                endRecord();
            }
        }

        if (userSettings.CaptureRawRecord) {
            if (hasMoreData) {
                if (rawBuffer.Position == 0) {
                    rawRecord =
                            new String(
                                    dataBuffer.Buffer,
                                    dataBuffer.LineStart,
                                    dataBuffer.Position - dataBuffer.LineStart - 1);
                    nextOffset = nextOffset + dataBuffer.Position - dataBuffer.LineStart;
                } else {
                    rawRecord =
                            new String(rawBuffer.Buffer, 0, rawBuffer.Position)
                                    + new String(
                                            dataBuffer.Buffer,
                                            dataBuffer.LineStart,
                                            dataBuffer.Position - dataBuffer.LineStart - 1);
                    nextOffset =
                            nextOffset
                                    + rawBuffer.Position
                                    + dataBuffer.Position
                                    - dataBuffer.LineStart;
                }
            } else {
                // for hasMoreData to ever be false, all data would have had to
                // have been
                // copied to the raw buffer
                rawRecord = new String(rawBuffer.Buffer, 0, rawBuffer.Position);
                nextOffset = nextOffset + rawBuffer.Position;
            }
        } else {
            rawRecord = "";
        }

        return hasReadNextLine;
    }

    /** @throws IOException Thrown if an error occurs while reading data from the source stream. */
    private void checkDataLength() throws IOException {
        if (!initialized) {
            if (fileName != null) {
                inputStream =
                        new BufferedReader(
                                new InputStreamReader(
                                        Files.newInputStream(Paths.get(fileName)), charset),
                                StaticSettings.MAX_FILE_BUFFER_SIZE);
            }

            charset = null;
            initialized = true;
        }

        updateCurrentValue();

        if (userSettings.CaptureRawRecord && dataBuffer.Count > 0) {
            if (rawBuffer.Buffer.length - rawBuffer.Position
                    < dataBuffer.Count - dataBuffer.LineStart) {
                int newLength =
                        rawBuffer.Buffer.length
                                + Math.max(
                                        dataBuffer.Count - dataBuffer.LineStart,
                                        rawBuffer.Buffer.length);

                char[] holder = new char[newLength];

                System.arraycopy(rawBuffer.Buffer, 0, holder, 0, rawBuffer.Position);

                rawBuffer.Buffer = holder;
            }

            System.arraycopy(
                    dataBuffer.Buffer,
                    dataBuffer.LineStart,
                    rawBuffer.Buffer,
                    rawBuffer.Position,
                    dataBuffer.Count - dataBuffer.LineStart);

            rawBuffer.Position += dataBuffer.Count - dataBuffer.LineStart;
        }

        try {
            dataBuffer.Count = inputStream.read(dataBuffer.Buffer, 0, dataBuffer.Buffer.length);
        } catch (IOException ex) {
            close();

            throw ex;
        }

        // if no more data could be found, set flag stating that
        // the end of the data was found

        // 如果没有更多的数据读取了，则认为文件读完了
        if (dataBuffer.Count == -1) {
            hasMoreData = false;
        }

        // 重置偏移量
        dataBuffer.Position = 0;
        dataBuffer.LineStart = 0;
        dataBuffer.ColumnStart = 0;
    }

    /**
     * Read the first record of data as column headers.
     *
     * @throws IOException Thrown if an error occurs while reading data from the source stream.
     */
    public void readHeaders() throws IOException {
        boolean result = readRecord();

        // copy the header data from the column array
        // to the header string array

        headersHolder.Length = columnsCount;

        headersHolder.Headers = new String[columnsCount];

        for (int i = 0; i < headersHolder.Length; i++) {
            String columnValue = get(i);

            headersHolder.Headers[i] = columnValue;

            // if there are duplicate header names, we will save the last one
            headersHolder.IndexByName.put(columnValue, i);
        }

        if (result) {
            currentRecord--;
        }

        columnsCount = 0;
    }

    /**
     * Returns the column header value for a given column index.
     *
     * @param columnIndex The index of the header column being requested.
     * @return The value of the column header at the given column index.
     * @throws IOException Thrown if this object has already been closed.
     */
    public String getHeader(int columnIndex) throws IOException {
        checkClosed();

        // check to see if we have read the header record yet

        // check to see if the column index is within the bounds
        // of our header array

        if (columnIndex > -1 && columnIndex < headersHolder.Length) {
            // return the processed header data for this column

            return headersHolder.Headers[columnIndex];
        } else {
            return "";
        }
    }

    public boolean isQualified(int columnIndex) throws IOException {
        checkClosed();

        if (columnIndex < columnsCount && columnIndex > -1) {
            return isQualified[columnIndex];
        } else {
            return false;
        }
    }

    /**
     * @throws IOException Thrown if a very rare extreme exception occurs during parsing, normally
     *     resulting from improper data format.
     */
    private void endColumn() throws IOException {
        String currentValue = "";

        // must be called before setting startedColumn = false
        if (startedColumn) {
            if (columnBuffer.Position == 0) {
                // columnBuffer 中没有缓存
                if (dataBuffer.ColumnStart < dataBuffer.Position) {
                    int lastLetter = dataBuffer.Position - 1;

                    if (userSettings.TrimWhitespace && !startedWithQualifier) {
                        while (lastLetter >= dataBuffer.ColumnStart
                                && (dataBuffer.Buffer[lastLetter] == Letters.SPACE
                                        || dataBuffer.Buffer[lastLetter] == Letters.TAB)) {
                            lastLetter--;
                        }
                    }

                    currentValue =
                            new String(
                                    dataBuffer.Buffer,
                                    dataBuffer.ColumnStart,
                                    lastLetter - dataBuffer.ColumnStart + 1);
                }
            } else {
                // columnBuffer 中存在缓存，将当前数据缓冲区的剩余字段的字符填入字段缓存中，并从中获取字段的值
                updateCurrentValue();

                int lastLetter = columnBuffer.Position - 1;

                // 去空格
                if (userSettings.TrimWhitespace && !startedWithQualifier) {
                    while (lastLetter >= 0
                            && (columnBuffer.Buffer[lastLetter] == Letters.SPACE
                                    || columnBuffer.Buffer[lastLetter] == Letters.SPACE)) {
                        lastLetter--;
                    }
                }

                currentValue = new String(columnBuffer.Buffer, 0, lastLetter + 1);
            }
        }

        columnBuffer.Position = 0;

        startedColumn = false;

        if (columnsCount >= 100000 && userSettings.SafetySwitch) {
            close();

            throw new IOException(
                    "Maximum column count of 100,000 exceeded in record "
                            + NumberFormat.getIntegerInstance().format(currentRecord)
                            + ". Set the SafetySwitch property to false"
                            + " if you're expecting more than 100,000 columns per record to"
                            + " avoid this error.");
        }

        // check to see if our current holder array for
        // column chunks is still big enough to handle another
        // column chunk

        if (columnsCount == values.length) {
            // holder array needs to grow to be able to hold another column
            int newLength = values.length * 2;

            String[] holder = new String[newLength];

            System.arraycopy(values, 0, holder, 0, values.length);

            values = holder;

            boolean[] qualifiedHolder = new boolean[newLength];

            System.arraycopy(isQualified, 0, qualifiedHolder, 0, isQualified.length);

            isQualified = qualifiedHolder;
        }

        values[columnsCount] = currentValue;

        isQualified[columnsCount] = startedWithQualifier;

        currentValue = "";

        columnsCount++;
    }

    private void appendLetter(char letter) {
        if (columnBuffer.Position == columnBuffer.Buffer.length) {
            int newLength = columnBuffer.Buffer.length * 2;

            char[] holder = new char[newLength];

            System.arraycopy(columnBuffer.Buffer, 0, holder, 0, columnBuffer.Position);

            columnBuffer.Buffer = holder;
        }
        columnBuffer.Buffer[columnBuffer.Position++] = letter;
        dataBuffer.ColumnStart = dataBuffer.Position + 1;
    }

    /** 更新当前值，缓存 */
    private void updateCurrentValue() {
        if (startedColumn && dataBuffer.ColumnStart < dataBuffer.Position) {
            // 若已经开始读取一个字段，并且字段的起始偏移量比当前读取到的偏移量要小时
            if (columnBuffer.Buffer.length - columnBuffer.Position
                    < dataBuffer.Position - dataBuffer.ColumnStart) {
                // 如果字段缓存的长度减去当前读取到的偏移量小于当前已经读取的长度，对 columnBuffer 进行扩容
                int newLength =
                        columnBuffer.Buffer.length
                                + Math.max(
                                        dataBuffer.Position - dataBuffer.ColumnStart,
                                        columnBuffer.Buffer.length);

                char[] holder = new char[newLength];

                System.arraycopy(columnBuffer.Buffer, 0, holder, 0, columnBuffer.Position);

                columnBuffer.Buffer = holder;
            }

            System.arraycopy(
                    dataBuffer.Buffer,
                    dataBuffer.ColumnStart,
                    columnBuffer.Buffer,
                    columnBuffer.Position,
                    dataBuffer.Position - dataBuffer.ColumnStart);

            // 字段缓存的偏移量为缓存的数据大小
            columnBuffer.Position += dataBuffer.Position - dataBuffer.ColumnStart;
        }

        dataBuffer.ColumnStart = dataBuffer.Position + 1;
    }

    /** @throws IOException Thrown if an error occurs while reading data from the source stream. */
    private void endRecord() throws IOException {
        // 将 hasReadNextLine 标志置 true 用于跳出循环

        hasReadNextLine = true;
        currentRecord++;
    }

    /**
     * Gets the corresponding column index for a given column header name.
     *
     * @param headerName The header name of the column.
     * @return The column index for the given column header name.&nbsp;Returns -1 if not found.
     * @throws IOException Thrown if this object has already been closed.
     */
    public int getIndex(String headerName) throws IOException {
        checkClosed();

        Integer indexValue = headersHolder.IndexByName.get(headerName);

        if (indexValue != null) {
            return indexValue;
        } else {
            return -1;
        }
    }

    public boolean skipRecord() throws IOException {
        checkClosed();

        boolean recordRead = false;

        if (hasMoreData) {
            recordRead = readRecord();

            if (recordRead) {
                currentRecord--;
            }
        }

        return recordRead;
    }

    /**
     * Skips the next line of data using the standard end of line characters and does not do any
     * column delimited parsing.
     *
     * @return Whether a line was successfully skipped or not.
     * @throws IOException Thrown if an error occurs while reading data from the source stream.
     */
    public boolean skipLine() throws IOException {
        checkClosed();

        // clear public column values for current line

        columnsCount = 0;

        boolean skippedLine = false;

        if (hasMoreData) {
            boolean foundEol = false;

            do {
                if (dataBuffer.Position == dataBuffer.Count) {
                    checkDataLength();
                } else {
                    skippedLine = true;

                    // grab the current letter as a char

                    char currentLetter = dataBuffer.Buffer[dataBuffer.Position];

                    if (currentLetter == Letters.CR || currentLetter == Letters.LF) {
                        foundEol = true;
                    }

                    // keep track of the last letter because we need
                    // it for several key decisions

                    lastLetter = currentLetter;

                    if (!foundEol) {
                        dataBuffer.Position++;
                    }
                } // end else
            } while (hasMoreData && !foundEol);

            columnBuffer.Position = 0;

            dataBuffer.LineStart = dataBuffer.Position + 1;
        }

        rawBuffer.Position = 0;
        rawRecord = "";

        return skippedLine;
    }

    /** Closes and releases all related resources. */
    public void close() {
        if (!closed) {
            close(true);

            closed = true;
        }
    }

    /** */
    private void close(boolean closing) {
        if (!closed) {
            if (closing) {
                charset = null;
                headersHolder.Headers = null;
                headersHolder.IndexByName = null;
                dataBuffer.Buffer = null;
                columnBuffer.Buffer = null;
                rawBuffer.Buffer = null;
            }

            try {
                if (initialized) {
                    inputStream.close();
                }
            } catch (Exception e) {
                // just eat the exception
            }

            inputStream = null;

            closed = true;
        }
    }

    /** @throws IOException Thrown if this object has already been closed. */
    private void checkClosed() throws IOException {
        if (closed) {
            throw new IOException("This instance of the CsvReader class has already been closed.");
        }
    }

    /** */
    @Override
    protected void finalize() {
        close(false);
    }

    private static class ComplexEscape {
        private static final int UNICODE = 1;

        private static final int OCTAL = 2;

        private static final int DECIMAL = 3;

        private static final int HEX = 4;
    }

    private static char hexToDec(char hex) {
        char result;

        if (hex >= 'a') {
            result = (char) (hex - 'a' + 10);
        } else if (hex >= 'A') {
            result = (char) (hex - 'A' + 10);
        } else {
            result = (char) (hex - '0');
        }

        return result;
    }

    private static class DataBuffer {
        // 缓存的数据，默认大小为1024
        public char[] Buffer;

        // 读取的偏移量
        public int Position;

        // 单次从流中读取并储存到缓存的数据量大小，可能小于缓存长度。
        public int Count;

        // / <summary>
        // / The position of the cursor in the buffer when the
        // / current column was started or the last time data
        // / was moved out to the column buffer.
        // / </summary>
        public int ColumnStart;

        public int LineStart;

        public DataBuffer() {
            Buffer = new char[StaticSettings.MAX_BUFFER_SIZE];
            Position = 0;
            Count = 0;
            ColumnStart = 0;
            LineStart = 0;
        }
    }

    private static class ColumnBuffer {
        public char[] Buffer;

        public int Position;

        public ColumnBuffer() {
            Buffer = new char[StaticSettings.INITIAL_COLUMN_BUFFER_SIZE];
            Position = 0;
        }
    }

    private static class RawRecordBuffer {
        public char[] Buffer;

        public int Position;

        public RawRecordBuffer() {
            Buffer =
                    new char
                            [StaticSettings.INITIAL_COLUMN_BUFFER_SIZE
                                    * StaticSettings.INITIAL_COLUMN_COUNT];
            Position = 0;
        }
    }

    private static class Letters {
        public static final char LF = '\n';

        public static final char CR = '\r';

        public static final char QUOTE = '"';

        public static final char COMMA = ',';

        public static final char SPACE = ' ';

        public static final char TAB = '\t';

        public static final char POUND = '#';

        public static final char BACKSLASH = '\\';

        public static final char NULL = '\0';

        public static final char BACKSPACE = '\b';

        public static final char FORM_FEED = '\f';

        public static final char ESCAPE = '\u001B'; // ASCII/ANSI escape

        public static final char VERTICAL_TAB = '\u000B';

        public static final char ALERT = '\u0007';
    }

    private class UserSettings {
        // having these as publicly accessible members will prevent
        // the overhead of the method call that exists on properties
        public boolean CaseSensitive;

        public char TextQualifier;

        public boolean TrimWhitespace;

        public boolean UseTextQualifier;

        public char Delimiter;

        public char RecordDelimiter;

        public char Comment;

        public boolean UseComments;

        public int EscapeMode;

        public boolean SafetySwitch;

        public boolean SkipEmptyRecords;

        public boolean CaptureRawRecord;

        public UserSettings() {
            CaseSensitive = true;
            TextQualifier = Letters.QUOTE;
            TrimWhitespace = true;
            UseTextQualifier = false;
            Delimiter = Letters.COMMA;
            RecordDelimiter = Letters.NULL;
            Comment = Letters.POUND;
            UseComments = false;
            EscapeMode = ReaderUtil.ESCAPE_MODE_DOUBLED;
            SafetySwitch = true;
            SkipEmptyRecords = true;
            CaptureRawRecord = true;
        }
    }

    private static class HeadersHolder {
        public String[] Headers;

        public int Length;

        public HashMap<String, Integer> IndexByName;

        public HeadersHolder() {
            Headers = null;
            Length = 0;
            IndexByName = Maps.newHashMap();
        }
    }

    private static class StaticSettings {
        // these are static instead of final so they can be changed in unit test
        // isn't visible outside this class and is only accessed once during
        // CsvReader construction
        public static final int MAX_BUFFER_SIZE = 1024;

        public static final int MAX_FILE_BUFFER_SIZE = 4 * 1024;

        public static final int INITIAL_COLUMN_COUNT = 10;

        public static final int INITIAL_COLUMN_BUFFER_SIZE = 50;
    }

    public long getNextOffset() {
        return nextOffset;
    }

    public void setNextOffset(long nextOffset) {
        this.nextOffset = nextOffset;
    }
}
