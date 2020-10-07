namespace HDBPublic
{
    using System;
    using System.Collections.Generic;

    enum PredicateType
    {
        EQ,   // Equal to.
        GT,   // Greater than.
        GE,   // Greater than or Equal.
        LT,   // Less than.
        LE,   // Less than or Equal.
        LIKE, // Like.

    }

    class PredicateTypeHelper
    {
        
        public static bool TryParse(string predicateTypeString, out PredicateType predicateType)
        {
            bool success = false;
            predicateType = PredicateType.EQ;
            if (m_PredicateTypeMap.ContainsKey(predicateTypeString))
            {
                predicateType = m_PredicateTypeMap[predicateTypeString];
                success = true;
            }
            return success;
        }

        private readonly static Dictionary<string, PredicateType> m_PredicateTypeMap = new Dictionary<string, PredicateType>(StringComparer.CurrentCultureIgnoreCase){
            {"=", PredicateType.EQ},
            {">", PredicateType.GT},
            {">=", PredicateType.GE},
            {"<", PredicateType.LT},
            {"<=", PredicateType.LE},
            {"like", PredicateType.LIKE},
        };
    }

}