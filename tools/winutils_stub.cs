using System;

namespace LocalWinUtilsStub
{
    internal static class Program
    {
        private static int Main(string[] args)
        {
            if (args.Length == 0)
            {
                return 0;
            }

            string command = args[0].ToLowerInvariant();

            if (command == "chmod" || command == "chown" || command == "groups")
            {
                return 0;
            }

            if (command == "ls")
            {
                if (args.Length > 1)
                {
                    Console.WriteLine("-rwxrwxrwx 1 owner group 0 Jan 1 00:00 " + args[1]);
                }
                return 0;
            }

            if (command == "systeminfo")
            {
                Console.WriteLine("Local winutils stub");
                return 0;
            }

            return 0;
        }
    }
}
