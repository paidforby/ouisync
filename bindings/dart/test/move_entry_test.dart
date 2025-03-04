import 'dart:convert';
import 'dart:io' as io;

import 'package:ouisync_plugin/ouisync_plugin.dart';
import 'package:test/test.dart';

void main() {
  late io.Directory temp;
  late Session session;
  late Repository repository;

  final folder1Path = '/folder1';
  final folder2Path = '/folder1/folder2';
  final folder2RootPath = '/folder2';
  final file1InFolder2Path = '/folder1/folder2/file1.txt';
  final fileContent = 'hello world';

  setUp(() async {
    temp = await io.Directory.systemTemp.createTemp();
    session = Session.create(configPath: '${temp.path}/device_id.conf');
    repository = await Repository.create(session,
        store: '${temp.path}/repo.db', readPassword: null, writePassword: null);
  });

  tearDown(() async {
    await repository.close();
    await session.close();
    await temp.delete(recursive: true);
  });

  test('Move folder ok when folder to move is empty', () async {
    // Create folder1 (/folder1) and folder2 inside folder1 (/folder1/folder2)
    {
      await Directory.create(repository, folder1Path);
      print('New folder: $folder1Path');

      await Directory.create(repository, folder2Path);
      print('New folder: $folder2Path');
    }
    // Check that root (/) contains only one entry (/file1)
    {
      final rootContents = await Directory.open(repository, '/');
      expect(rootContents.toList().length, equals(1));

      print('Root contents: ${rootContents.toList()}');
    }
    // Check that folder2 (/folder1/folder2) is empty
    {
      final rootContents = await Directory.open(repository, folder2Path);
      expect(rootContents.toList().length, equals(0));

      print('Folder2 contents: ${rootContents.toList()}');
    }
    // Move folder2 (/folder1/folder2) to root (/folder2)
    {
      print('Moving folder: src: $folder2Path - dst: $folder2RootPath');
      await repository.move(folder2Path, folder2RootPath);
    }
    // Check the contents in root for two entries: folder1 (/folder1) and folder2 (/folder2)
    {
      final rootContentsAfterMovingFolder2 =
          await Directory.open(repository, '/');
      expect(rootContentsAfterMovingFolder2.isNotEmpty, equals(true));
      expect(rootContentsAfterMovingFolder2.toList().length, equals(2));

      print(
          'Root contents after move: ${rootContentsAfterMovingFolder2.toList()}');
    }
  });

  test('Move folder ok when folder to move is not empty', () async {
    // Create folder1 (/folder1) and folder2 inside folder1 (/folder1/folder2)
    {
      await Directory.create(repository, folder1Path);
      print('New folder: $folder1Path');

      await Directory.create(repository, folder2Path);
      print('New folder: $folder2Path');
    }
    // Check that root (/) contains only one entry (/file1)
    {
      final rootContents = await Directory.open(repository, '/');
      expect(rootContents.toList().length, equals(1));

      print('Root contents: ${rootContents.toList()}');
    }
    // Create new file1.txt in folder2 (/folder1/folder2/file1.txt)
    {
      final file = await File.create(repository, file1InFolder2Path);
      await file.write(0, utf8.encode(fileContent));
      await file.close();
    }
    // Check that folder2 (/folder1/folder2) contains only one entry (/folder1/folder2/file1.txt)
    {
      final folder2Contents = await Directory.open(repository, folder2Path);
      expect(folder2Contents.toList().length, equals(1));

      print('Folder2 contents: ${folder2Contents.toList()}');
    }
    // Move folder2 (/folder1/folder2) to root (/folder2) when folder2 is not empty (/folder1/folder2/file1.txt)
    {
      print('Moving folder: src: $folder2Path - dst: $folder2RootPath');
      await repository.move(folder2Path, folder2RootPath);
    }
    // Check the contents in root for two entryes: folder1 (/folder1) and folder2 (/folder2)
    {
      final rootContentsAfterMovingFolder2 =
          await Directory.open(repository, '/');
      expect(rootContentsAfterMovingFolder2.isNotEmpty, equals(true));
      expect(rootContentsAfterMovingFolder2.toList().length, equals(2));

      print(
          'Root contents after move: ${rootContentsAfterMovingFolder2.toList()}');
    }
  });
}
