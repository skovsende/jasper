﻿using System;
using BlueMilk.Util;

namespace BlueMilk.Codegen.ServiceLocation
{
    public class NoArgConcreteCreator : IVariableSource
    {
        public bool Matches(Type type)
        {
            return type.IsConcreteWithDefaultCtor();
        }

        public Variable Create(Type type)
        {
            return new NoArgCreationVariable(type);
        }
    }
}