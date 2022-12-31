import os
import subprocess
from itertools import groupby
from operator import itemgetter
from typing import List, Optional, Tuple, Union


from dmoj import executors
from dmoj.commands.base_command import Command
from dmoj.error import CompileError, InvalidCommandException, OutputLimitExceeded
from dmoj.graders import StandardGrader
from dmoj.judgeenv import get_problem_root, get_supported_problems
from dmoj.problem import BatchedTestCase, Problem, ProblemConfig, ProblemDataManager, TestCase
from dmoj.result import CheckerResult, Result
from dmoj.utils.ansi import print_ansi
from dmoj.utils.unicode import utf8bytes, utf8text


all_executors = executors.executors


class ValidationGrader(StandardGrader):
    def _interact_with_process(self, case, result, input):
        process = self._current_proc
        try:
            result.proc_output, error = process.communicate(
                input, outlimit=case.config.output_limit_length, errlimit=1048576
            )
        except OutputLimitExceeded:
            error = b''
            process.kill()
        finally:
            process.wait()
        result.proc_error = error
        return error

    def _launch_process(self, case):
        self._current_proc = self.binary.launch(
            str(case.batch),
            str(case.position),
            time=self.problem.time_limit,
            memory=self.problem.memory_limit,
            symlinks=case.config.symlinks,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            wall_time=case.config.wall_time_factor * self.problem.time_limit,
        )

    def check_result(self, case, result):
        return CheckerResult(
            not result.result_flag, (not result.result_flag) * case.points, utf8text(result.proc_error.rstrip())
        )


class ValidateCommand(Command):
    name = 'validate'
    help = 'Validates input for problems.'

    def _populate_parser(self) -> None:
        self.arg_parser.add_argument('problem_ids', nargs='+', help='ids of problems to test')

    def execute(self, line: str) -> int:
        args = self.arg_parser.parse_args(line)

        problem_ids = args.problem_ids
        supported_problems = set(get_supported_problems())

        unknown_problems = ', '.join(f"'{i}'" for i in problem_ids if i not in supported_problems)
        if unknown_problems:
            raise InvalidCommandException(f'unknown problem(s) {unknown_problems}')
        total_fails = 0
        for problem_id in problem_ids:
            fails = self.validate_problem(problem_id)
            if fails:
                print_ansi(f'Problem #ansi[{problem_id}](cyan|bold) #ansi[failed](red|bold).')
                total_fails += 1
            else:
                print_ansi(f'Problem #ansi[{problem_id}](cyan|bold) passed with flying colours.')
            print()

        print()
        print('Test complete.')
        if total_fails:
            print_ansi(f'#ansi[A total of {total_fails} problem(s) have invalid input](red|bold)')
        else:
            print_ansi('#ansi[All problems validated.](green|bold)')

        return total_fails

    def validate_problem(self, problem_id: str) -> int:
        print_ansi(f'Validating problem #ansi[{problem_id}](cyan|bold)...')

        problem_root = get_problem_root(problem_id)
        config = ProblemConfig(ProblemDataManager(problem_root))

        if not config or 'validator' not in config:
            print_ansi('\t#ansi[Skipped](magenta|bold) - No validator found')
            return 0

        validator_config = config['validator']
        language = validator_config['language']
        if language not in all_executors:
            print_ansi('\t\t#ansi[Skipped](magenta|bold) - Language not supported')
            return 0
        time_limit = validator_config['time']
        memory_limit = validator_config['memory']
        with open(os.path.join(problem_root, validator_config['source'])) as f:
            source = f.read()

        problem = Problem(problem_id, time_limit, memory_limit, {})

        try:
            real_grader = problem.grader_class(self.judge, problem, language, utf8bytes(source))
            validation_grader = ValidationGrader(self.judge, problem, language, utf8bytes(source))
        except CompileError as compilation_error:
            error = compilation_error.message or 'compiler exited abnormally'
            print_ansi('#ansi[Failed compiling validator!](red|bold)')
            print(error.rstrip())
            return 1

        flattened_cases: List[Tuple[Optional[int], Union[TestCase, BatchedTestCase]]] = []
        batch_number = 0
        for case in real_grader.cases():
            if isinstance(case, BatchedTestCase):
                batch_number += 1
                for batched_case in case.batched_cases:
                    flattened_cases.append((batch_number, batched_case))
            else:
                flattened_cases.append((None, case))

        case_number = 0
        fail = 0
        for batch_number, cases in groupby(flattened_cases, key=itemgetter(0)):
            if batch_number:
                print_ansi(f'#ansi[Batch #{batch_number}](yellow|bold)')
            for _, case in cases:
                case_number += 1
                result = validation_grader.grade(case)

                code = result.readable_codes()[0]
                colored_code = f'#ansi[{code}]({Result.COLORS_BYID[code]}|bold)'
                colored_feedback = f'(#ansi[{utf8text(result.feedback)}](|underline))' if result.feedback else ''
                case_padding = '  ' if batch_number is not None else ''
                print_ansi(f'{case_padding}Test case {case_number:2d} {colored_code:3s} {colored_feedback}')

                if result.result_flag:
                    fail = 1

        return fail
