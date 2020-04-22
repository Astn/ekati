using System;
using System.Runtime.InteropServices;
using RocksDbSharp;

namespace Ahghee.Grpc
{
    public static class Merge
    {
        private static Object locker = new { };
        private static Object holder = null;
        public static MergeOperator Create()
        {
            unsafe
            {
                MergeOperators.PartialMergeFunc partial = Merge.PartialMergeFunc;
                MergeOperators.FullMergeFunc full = FullMergeFunc;
                MergeOperators.DeleteValueFunc del = DeleteValueFunc;
                lock (locker)
                {
                    if (holder == null)
                    {
                        holder = new
                        {
                            partial,
                            full,
                            del
                        };    
                    }
                }
                
                return MergeOperators.Create("pointers", partial, full, del);
            }
        }
        /// <summary>
        /// This function performs merge(left_op, right_op)
        /// when both the operands are themselves merge operation types.
        /// Save the result in *new_value and return true. If it is impossible
        /// or infeasible to combine the two operations, return false instead.
        /// This is called to combine two-merge operands (if possible)
        /// </summary>
        /// <param name="key">The key that's associated with this merge operation</param>
        /// <param name="keyLength"></param>
        /// <param name="operandsList">the sequence of merge operations to apply, front() first</param>
        /// <param name="operandsListLength"></param>
        /// <param name="numOperands"></param>
        /// <param name="success">Client is responsible for filling the merge result here</param>
        /// <param name="newValueLength"></param>
        /// <returns></returns>
        public static IntPtr PartialMergeFunc(IntPtr key, UIntPtr keyLength, IntPtr operandsList, IntPtr operandsListLength, int numOperands, IntPtr success, IntPtr newValueLength)
        {
            unsafe
            {
                // TODO: deduplicate
                // for each operand
                long totalLength = 0;
                var size = sizeof(IntPtr);
                var orig = operandsListLength;
                for (int i = 0; i < numOperands; i++)
                {
                    totalLength +=  Marshal.ReadIntPtr(orig).ToInt64();
                    var added = IntPtr.Add(orig, size);
                    orig = added;
                    // sum all the lengths so we can allocate once.
                }

                Int64 tots = totalLength;
                Int64* ptots = &tots;
                IntPtr lenPtr = new IntPtr(ptots);
                IntPtr dest = Marshal.AllocHGlobal( new IntPtr( totalLength ));

                // for each operand
                var destIter = dest;
                var copied = 0L;
                for (int i = 0; i < numOperands; i++)
                {
                    var cntToCopy = Marshal.ReadInt64(IntPtr.Add(operandsListLength, i * size));
                    // copy each operand
                    var operandLoc = IntPtr.Add(operandsList, i * size);
                    Buffer.MemoryCopy(Marshal.ReadIntPtr( operandLoc).ToPointer(),  destIter.ToPointer(), totalLength - copied, cntToCopy );
                    copied += cntToCopy;
                    destIter = IntPtr.Add(destIter, Convert.ToInt32(cntToCopy));
                }

                Marshal.WriteInt64(newValueLength, Convert.ToInt32(totalLength));
                Marshal.WriteInt64(success, 1);
                return dest;
            }
        }
        /// <summary>
        /// Gives the client a way to express the read -> modify -> write semantics.
        /// Called when a Put/Delete is the *existing_value (or nullptr)
        /// </summary>
        /// <param name="key">The key that's associated with this merge operation.</param>
        /// <param name="keyLength"></param>
        /// <param name="existingValue">null indicates that the key does not exist before this op</param>
        /// <param name="existingValueLength"></param>
        /// <param name="operandsList">the sequence of merge operations to apply, front() first.</param>
        /// <param name="operandsListLength"></param>
        /// <param name="numOperands"></param>
        /// <param name="success">Client is responsible for filling the merge result here</param>
        /// <param name="newValueLength"></param>
        /// <returns></returns>
        public static IntPtr FullMergeFunc(IntPtr key, UIntPtr keyLength, IntPtr existingValue,
            UIntPtr existingValueLength, 
            // list of pointers to operands
            IntPtr operandsList, 
            // list of lenghts for the above pointers
            IntPtr operandsListLength, 
            // number of pointers in the above two
            int numOperands,
            IntPtr success, IntPtr newValueLength)
        {
            unsafe
            {
                // all we have to do is a binary concatenation
                if (existingValueLength.ToUInt64() > 0)
                {
                    // create a buffer size of both lengths, and copy to it, then return it.
                    // TODO: deduplicate
                    var existingLen = Marshal.ReadInt64((IntPtr)existingValueLength.ToPointer());
                    var oll = Marshal.ReadInt64(operandsListLength);
                    var newsize = existingLen + oll;
                    IntPtr dest = Marshal.AllocHGlobal((IntPtr) newsize );
                    // copy existing
                    Buffer.MemoryCopy(existingValue.ToPointer(), dest.ToPointer(),newsize,existingLen);
                    
                    IntPtr destOffset = IntPtr.Add(dest, Convert.ToInt32( existingLen));
                    Buffer.MemoryCopy(operandsList.ToPointer(), destOffset.ToPointer(),oll,oll);
                    Marshal.WriteInt64(newValueLength, newsize);
                    Marshal.WriteInt32(success, 1);
                    return dest;
                }
                
                return PartialMergeFunc(key, keyLength, operandsList, operandsListLength, numOperands, success, newValueLength);
            }
        }

        public static void DeleteValueFunc(IntPtr value, UIntPtr valueLength)
        {
            valueLength = (UIntPtr) 0;
        }
    }
}