module arctic.lz4;

align(1) struct LZ4_stream_t
{
	uint[4104] table;
}

align(1) struct LZ4_streamDecode_t
{
	uint[4] table;
}

extern(C) int LZ4_compress (const(char)* source, char* dest, int inputSize);
extern(C) int LZ4_decompress_safe (const(char)* source, char* dest, int compressedSize, int maxOutputSize);
extern(C) int LZ4_compressBound (int isize);
extern(C) int LZ4_compress_limitedOutput (const(char)* source, char* dest, int inputSize, int maxOutputSize);
extern(C) int LZ4_decompress_fast (const(char)* source, char* dest, int originalSize);
extern(C) int LZ4_decompress_safe_partial (const(char)* source, char* dest, int compressedSize, int targetOutputSize, int maxOutputSize);
extern(C) void* LZ4_createStream();
extern(C) int LZ4_free (void* LZ4_stream);
extern(C) int LZ4_loadDict (void* LZ4_stream, const(char)* dictionary, int dictSize);
extern(C) int LZ4_compress_continue (void* LZ4_stream, const(char)* source, char* dest, int inputSize);
extern(C) int LZ4_compress_limitedOutput_continue (void* LZ4_stream, const(char)* source, char* dest, int inputSize, int maxOutputSize);
extern(C) int LZ4_saveDict (void* LZ4_stream, char* safeBuffer, int dictSize);
extern(C) void* LZ4_createStreamDecode();
extern(C) int LZ4_free (void* LZ4_stream);
extern(C) int LZ4_decompress_safe_continue (void* LZ4_streamDecode, const(char)* source, char* dest, int compressedSize, int maxOutputSize);
extern(C) int LZ4_decompress_fast_continue (void* LZ4_streamDecode, const(char)* source, char* dest, int originalSize);
extern(C) int LZ4_setDictDecode (void* LZ4_streamDecode, const(char)* dictionary, int dictSize);
extern(C) int LZ4_decompress_safe_usingDict (const(char)* source, char* dest, int compressedSize, int maxOutputSize, const(char)* dictStart, int dictSize);
extern(C) int LZ4_decompress_fast_usingDict (const(char)* source, char* dest, int originalSize, const(char)* dictStart, int dictSize);
extern(C) int LZ4_uncompress (const(char)* source, char* dest, int outputSize);
extern(C) int LZ4_uncompress_unknownOutputSize (const(char)* source, char* dest, int isize, int maxOutputSize);
extern(C) int LZ4_sizeofState();
extern(C) int LZ4_compress_withState (void* state, const(char)* source, char* dest, int inputSize);
extern(C) int LZ4_compress_limitedOutput_withState (void* state, const(char)* source, char* dest, int inputSize, int maxOutputSize);
extern(C) void* LZ4_create (const(char)* inputBuffer);
extern(C) int LZ4_sizeofStreamState();
extern(C) int LZ4_resetStreamState (void* state, const(char)* inputBuffer);
extern(C) char* LZ4_slideInputBuffer (void* state);
extern(C) int LZ4_decompress_safe_withPrefix64k (const(char)* source, char* dest, int compressedSize, int maxOutputSize);
extern(C) int LZ4_decompress_fast_withPrefix64k (const(char)* source, char* dest, int originalSize);