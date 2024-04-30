package org.equalitie.ouisync.example

import android.content.ContentResolver
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.os.Bundle
import android.os.Environment
import android.util.Log
import androidx.activity.ComponentActivity
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.compose.setContent
import androidx.activity.result.contract.ActivityResultContracts
import androidx.activity.viewModels
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Add
import androidx.compose.material.icons.filled.Check
import androidx.compose.material.icons.filled.Delete
import androidx.compose.material.icons.filled.Share
import androidx.compose.material.icons.filled.Warning
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.BottomAppBar
import androidx.compose.material3.Button
import androidx.compose.material3.Card
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.FloatingActionButton
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Scaffold
import androidx.compose.material3.SnackbarHost
import androidx.compose.material3.SnackbarHostState
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.TextField
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.lifecycle.ViewModel
import androidx.lifecycle.ViewModelProvider
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.equalitie.ouisync.Repository
import org.equalitie.ouisync.Session
import org.equalitie.ouisync.ShareToken
import java.io.File

private const val TAG = "ouisync.example"
private val PADDING = 8.dp

private val DB_EXTENSION = "ouisyncdb"

class MainActivity : ComponentActivity() {
    private val viewModel by viewModels<AppViewModel>() {
        AppViewModel.Factory(this)
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        setContent {
            App(viewModel)
        }
    }
}

class AppViewModel(private val configDir: String) : ViewModel() {
    class Factory(private val context: Context) : ViewModelProvider.Factory {
        @Suppress("UNCHECKED_CAST")
        override fun <T : ViewModel> create(modelClass: Class<T>): T {
            val rootDir = context.getFilesDir()
            val configDir = "$rootDir/config"
            //val storeDir = "$rootDir/store"

            return AppViewModel(configDir) as T
        }
    }

    var sessionError by mutableStateOf<String?>(null)
        private set

    var protocolVersion by mutableStateOf<Int>(0)
        private set

    var repositories by mutableStateOf<Map<String, Repository>>(mapOf())
        private set

    var storeDir by mutableStateOf<String?>(null)

    private var session: Session? = null

    init {
        try {
            session = Session.create(configDir)
            sessionError = null
        } catch (e: Exception) {
            Log.e(TAG, "Session.create failed", e)
            sessionError = e.toString()
        } catch (e: java.lang.Error) {
            Log.e(TAG, "Session.create failed", e)
            sessionError = e.toString()
        }

        viewModelScope.launch {
            session?.let {
                protocolVersion = it.currentProtocolVersion()
            }
            //openRepositories()
        }
    }


    suspend fun createRepository(name: String, token: String) {
        val session = this.session ?: return

        if (repositories.containsKey(name)) {
            Log.e(TAG, "repository named \"$name\" already exists")
            return
        }

        var shareToken: ShareToken? = null

        if (!token.isEmpty()) {
            shareToken = ShareToken.fromString(session, token)
        }

        val repo = Repository.create(
            session,
            "$storeDir/$name.$DB_EXTENSION",
            readSecret = null,
            writeSecret = null,
            shareToken = shareToken,
        )

        repo.setSyncEnabled(true)

        repositories = repositories + (name to repo)
    }

    suspend fun openRepository(name: String, token: String) {
        val session = this.session ?: return

        if (repositories.containsKey(name)) {
            Log.e(TAG, "repository named \"$name\" already exists")
            return
        }

        val repo = Repository.open(
            session,
            "$storeDir/$name.$DB_EXTENSION",
        )

        repo.setSyncEnabled(true)

        repositories = repositories + (name to repo)
    }


    suspend fun deleteRepository(name: String) {
        val repo = repositories.get(name) ?: return
        repositories = repositories - name

        repo.close()

        val baseName = "$name.$DB_EXTENSION"
        val files = File(storeDir).listFiles() ?: arrayOf()

        // A ouisync repository database consist of multiple files. Delete all of them.
        for (file in files) {
            if (file.getName().startsWith(baseName)) {
                file.delete()
            }
        }
    }

    private suspend fun openRepositories() {
        val session = this.session ?: return
        val files = File(storeDir).listFiles() ?: arrayOf()

        for (file in files) {
            if (file.getName().endsWith(".$DB_EXTENSION")) {
                try {
                    val name = file
                        .getName()
                        .substring(0, file.getName().length - DB_EXTENSION.length - 1)
                    val repo = Repository.open(session, file.getPath())

                    Log.i(TAG, "Opened repository $name")

                    repositories = repositories + (name to repo)
                } catch (e: Exception) {
                    Log.e(TAG, "Failed to open repository at ${file.getPath()}")
                    continue
                }
            }
        }
    }

    override fun onCleared() {
        val repos = repositories.values
        repositories = mapOf()

        viewModelScope.launch {
            for (repo in repos) {
                repo.close()
            }

            session?.close()
            session = null
        }
    }
}

fun checkUriPersisted(contentResolver: ContentResolver, uri: Uri): Boolean {
    return contentResolver.persistedUriPermissions.any { perm -> perm.uri == uri }
}

class PermissibleOpenDocumentTreeContract(
    private val write: Boolean = false,
) : ActivityResultContracts.OpenDocumentTree() {
    override fun createIntent(context: Context, input: Uri?): Intent {
        val intent = super.createIntent(context, input)
        intent.addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
        if (write) {
            intent.addFlags(Intent.FLAG_GRANT_WRITE_URI_PERMISSION)
        }
        intent.addFlags(Intent.FLAG_GRANT_PREFIX_URI_PERMISSION)
        intent.addFlags(Intent.FLAG_GRANT_PERSISTABLE_URI_PERMISSION)

        return intent
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun App(viewModel: AppViewModel) {
    val scope = rememberCoroutineScope()
    val snackbar = remember { Snackbar(scope) }
    var adding by remember { mutableStateOf(false) }
    var importing by remember { mutableStateOf(false) }

    MaterialTheme {
        Scaffold(
            floatingActionButton = {
                if (!adding) {
                    FloatingActionButton(
                        onClick = {
                            adding = true
                        },
                    ) {
                        Icon(Icons.Default.Add, "Add")
                    }
                }
            },
            bottomBar = { StatusBar(viewModel) },
            snackbarHost = { SnackbarHost(snackbar.state) },
            content = { padding ->
                Column(
                    modifier = Modifier
                        .fillMaxSize()
                        .padding(padding)
                        .padding(PADDING),
                ) {
                    viewModel.sessionError?.let {
                        Text(it)
                    }

                    Button(onClick = { importing = true}
                    ) {
                        Text(text = "Import Existing Repo")
                    }

                    RepositoryList(viewModel, snackbar = snackbar)

                    if (adding) {
                        OpenDirectoryDialog(
                            viewModel,
                            isImport = false,
                            snackbar = snackbar,
                            onDone = { adding = false },
                        )
                    }
                    if (importing) {
                        OpenDirectoryDialog(
                            viewModel,
                            isImport = true,
                            snackbar = snackbar,
                            onDone = { importing = false },
                        )
                    }
                }
            },
        )
    }
}

@Composable
fun StatusBar(viewModel: AppViewModel) {
    BottomAppBar {
        if (viewModel.sessionError == null) {
            Icon(Icons.Default.Check, "OK")
            Spacer(modifier = Modifier.weight(1f))
            Text("Protocol version: ${viewModel.protocolVersion}")
        } else {
            Icon(Icons.Default.Warning, "Error")
        }
    }
}

@Composable
fun RepositoryList(viewModel: AppViewModel, snackbar: Snackbar) {
    val scope = rememberCoroutineScope()

    LazyColumn(
        verticalArrangement = Arrangement.spacedBy(PADDING),
    ) {
        for (entry in viewModel.repositories) {
            item(key = entry.key) {
                RepositoryItem(
                    entry.key,
                    entry.value,
                    onDelete = {
                        scope.launch {
                            viewModel.deleteRepository(entry.key)
                            snackbar.show("Repository deleted")
                        }
                    },
                )
            }
        }
    }
}

@Composable
fun RepositoryItem(
    name: String,
    repository: Repository,
    onDelete: () -> Unit,
) {
    val scope = rememberCoroutineScope()
    val context = LocalContext.current
    var deleting by remember { mutableStateOf(false) }

    suspend fun sendShareToken() {
        val token = repository.createShareToken().toString()

        val sendIntent = Intent().apply {
            action = Intent.ACTION_SEND
            putExtra(Intent.EXTRA_TEXT, token)
            type = "text/plain"
        }
        val shareIntent = Intent.createChooser(sendIntent, null)

        context.startActivity(shareIntent)
    }

    Card(modifier = Modifier.fillMaxWidth()) {
        Row(modifier = Modifier.padding(PADDING)) {
            Text(name, fontWeight = FontWeight.Bold)

            Spacer(Modifier.weight(1f))

            IconButton(
                onClick = {
                    scope.launch {
                        sendShareToken()
                    }
                },
            ) {
                Icon(Icons.Default.Share, "Share")
            }

            IconButton(
                onClick = {
                    deleting = true
                },
            ) {
                Icon(Icons.Default.Delete, "Delete")
            }
        }
    }

    if (deleting) {
        AlertDialog(
            title = {
                Text("Delete repository")
            },
            text = {
                Text("Are you sure you want to delete this repository?")
            },
            onDismissRequest = { deleting = false },
            confirmButton = {
                TextButton(
                    onClick = {
                        onDelete()
                        deleting = false
                    },
                ) {
                    Text("Delete")
                }
            },
            dismissButton = {
                TextButton(
                    onClick = { deleting = false },
                ) {
                    Text("Cancel")
                }
            },
        )
    }
}

@Composable
fun OpenDirectoryDialog(
    viewModel: AppViewModel,
    isImport: Boolean,
    onDone: () -> Unit,
    snackbar: Snackbar,
) {
    val context = LocalContext.current
    val isDirectoryPicked = remember { mutableStateOf(false) }
    val dirPickerLauncher = rememberLauncherForActivityResult(
        contract = PermissibleOpenDocumentTreeContract(true),
        onResult = { maybeUri ->
            maybeUri?.let { uri ->
                val flags = Intent.FLAG_GRANT_READ_URI_PERMISSION or
                        Intent.FLAG_GRANT_WRITE_URI_PERMISSION
                if (checkUriPersisted(context.contentResolver, uri)) {
                    context.contentResolver.releasePersistableUriPermission(uri, flags)
                }
                context.contentResolver.takePersistableUriPermission(uri, flags)
                uri.path?.let { path ->
                    val split: List<String> = path.split(":".toRegex())
                    val file = File(Environment.getExternalStorageDirectory(), split[1])
                    viewModel.storeDir = file.path
                    isDirectoryPicked.value = true
                }
            }
        }
    )

    AlertDialog(
        title = { Text("Open Directory Picker") },
        confirmButton = {
            TextButton(
                onClick = { dirPickerLauncher.launch(Uri.EMPTY) },
            ) {
                Text("Continue")
            }
        },
        dismissButton = {
            TextButton(onClick = { onDone() }) {
                Text("Cancel")
            }
        },
        onDismissRequest = { onDone() },
    )
    if (isDirectoryPicked.value) {
        if (isImport) {
            ImportRepositoryDialog(
                viewModel,
                snackbar = snackbar,
                onDone = {
                    isDirectoryPicked.value = false
                    onDone()
                         },
            )
        }
        else {
            CreateRepositoryDialog(
                viewModel,
                snackbar = snackbar,
                onDone = {
                    isDirectoryPicked.value = false
                    onDone()
                         },
            )
        }
    }
}

@Composable
fun CreateRepositoryDialog(
    viewModel: AppViewModel,
    onDone: () -> Unit,
    snackbar: Snackbar,
) {
    var scope = rememberCoroutineScope()

    var name by remember {
        mutableStateOf("")
    }

    var nameError by remember {
        mutableStateOf("")
    }

    var token by remember {
        mutableStateOf("")
    }

    fun validate(): Boolean {
        if (name.isEmpty()) {
            nameError = "Name is missing"
            return false
        }

        if (viewModel.repositories.containsKey(name)) {
            nameError = "Name is already taken"
            return false
        }

        nameError = ""
        return true
    }

    AlertDialog(
        title = { Text("Create repository") },
        confirmButton = {
            TextButton(
                onClick = {
                    if (validate()) {
                        scope.launch {
                            try {
                                viewModel.createRepository(name, token)
                                snackbar.show("Repository created")
                            } catch (e: Exception) {
                                snackbar.show("Repository creation failed ($e)")
                            } finally {
                                onDone()
                            }
                        }
                    }
                },
            ) {
                Text("Create")
            }
        },
        dismissButton = {
            TextButton(onClick = { onDone() }) {
                Text("Cancel")
            }
        },
        onDismissRequest = { onDone() },
        text = {
            Column(verticalArrangement = Arrangement.spacedBy(PADDING)) {
                Text("Saving in ${viewModel.storeDir}")
                TextField(
                    value = name,
                    onValueChange = { name = it },
                    label = { Text("Name*") },
                    supportingText = {
                        if (!nameError.isEmpty()) {
                            Text(nameError)
                        }
                    },
                    isError = !nameError.isEmpty(),
                )

                TextField(
                    label = { Text("Token") },
                    value = token,
                    onValueChange = { token = it },
                )
            }
        },
    )
}

@Composable
fun ImportRepositoryDialog(
    viewModel: AppViewModel,
    onDone: () -> Unit,
    snackbar: Snackbar,
) {
    var scope = rememberCoroutineScope()

    var name by remember {
        mutableStateOf("")
    }

    var nameError by remember {
        mutableStateOf("")
    }

    var token by remember {
        mutableStateOf("")
    }

    fun validate(): Boolean {
        if (name.isEmpty()) {
            nameError = "Name is missing"
            return false
        }

        if (viewModel.repositories.containsKey(name)) {
            nameError = "Name is already taken"
            return false
        }

        nameError = ""
        return true
    }

    AlertDialog(
        title = { Text("Open repository") },
        confirmButton = {
            TextButton(
                onClick = {
                    if (validate()) {
                        scope.launch {
                            try {
                                viewModel.openRepository(name, token)
                                snackbar.show("Repository imported")
                            } catch (e: Exception) {
                                snackbar.show("Repository import failed ($e)")
                            } finally {
                                onDone()
                            }
                        }
                    }
                },
            ) {
                Text("Import")
            }
        },
        dismissButton = {
            TextButton(onClick = { onDone() }) {
                Text("Cancel")
            }
        },
        onDismissRequest = { onDone() },
        text = {
            Column(verticalArrangement = Arrangement.spacedBy(PADDING)) {
                Text("Importing from ${viewModel.storeDir}")
                TextField(
                    value = name,
                    onValueChange = { name = it },
                    label = { Text("Name*") },
                    supportingText = {
                        if (!nameError.isEmpty()) {
                            Text(nameError)
                        }
                    },
                    isError = !nameError.isEmpty(),
                )

                TextField(
                    label = { Text("Token") },
                    value = token,
                    onValueChange = { token = it },
                )
            }
        },
    )
}


class Snackbar(val scope: CoroutineScope) {
    val state = SnackbarHostState()

    fun show(text: String) {
        scope.launch {
            state.showSnackbar(text, withDismissAction = true)
        }
    }
}
