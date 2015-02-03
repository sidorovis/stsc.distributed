package stsc.frontend.zozka.panes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.TimeZone;

import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.BorderPane;
import javafx.stage.Stage;

import org.controlsfx.dialog.Dialogs;

import stsc.frontend.zozka.dialogs.DatePickerDialog;
import stsc.frontend.zozka.gui.models.feedzilla.FeedzillaArticleDescription;
import stsc.frontend.zozka.panes.internal.ProgressWithStopPane;
import stsc.frontend.zozka.settings.ControllerHelper;
import stsc.news.feedzilla.FeedzillaFileStorage;
import stsc.news.feedzilla.FeedzillaHashStorage;
import stsc.news.feedzilla.file.schema.FeedzillaFileArticle;

public class FeedzillaArticlesPane extends BorderPane {

	static {
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
	}

	final private BorderPane mainPane = new BorderPane();
	private Stage owner;

	@FXML
	private Label datafeedLabel;

	private ObservableList<FeedzillaArticleDescription> model = FXCollections.observableArrayList();
	@FXML
	private TableView<FeedzillaArticleDescription> newsTable;
	@FXML
	private TableColumn<FeedzillaArticleDescription, String> dateColumn;

	private final ProgressWithStopPane progressWithStopPane = new ProgressWithStopPane(false);

	public FeedzillaArticlesPane() throws IOException {
		final Parent gui = initializeGui();
		validateGui();
		setUpTable();
		mainPane.setCenter(gui);
		mainPane.setBottom(null);
	}

	private void setUpTable() {
		newsTable.setItems(model);
		dateColumn.setCellValueFactory(new PropertyValueFactory<FeedzillaArticleDescription, String>("date"));
	}

	private Parent initializeGui() throws IOException {
		final URL location = FeedzillaArticlesPane.class.getResource("05_zozka_feedzilla_visualiser_pane.fxml");
		final FXMLLoader loader = new FXMLLoader(location);
		loader.setController(this);
		final Parent result = loader.load();
		return result;
	}

	private void validateGui() {
		assert newsTable != null : "fx:id=\"newsTable\" was not injected: check your FXML file.";
		assert dateColumn != null : "fx:id=\"dateColumn\" was not injected: check your FXML file.";
		assert datafeedLabel != null : "fx:id=\"datafeedLabel\" was not injected: check your FXML file.";
	}

	@FXML
	private void datafeedClicked(MouseEvent event) {
		if (event.getClickCount() == 2) {
			try {
				chooseFolder();
			} catch (Exception e) {
				Dialogs.create().owner(owner).showException(e);
			}
		}
	}

	private void chooseFolder() throws FileNotFoundException, IOException {
		if (ControllerHelper.chooseFolder(owner, datafeedLabel)) {
			chooseDate();
		}
	}

	private void chooseDate() throws FileNotFoundException, IOException {
		final DatePickerDialog pickDate = new DatePickerDialog("Choose Date", owner, LocalDate.of(1990, 1, 1));
		pickDate.showAndWait();
		if (pickDate.isOk()) {
			loadFeedzillaFileStorage(pickDate.getDate().atStartOfDay());
		}
	}

	private void loadFeedzillaFileStorage(LocalDateTime dateDownloadFrom) {
		progressWithStopPane.show();
		progressWithStopPane.setIndicatorProgress(0.0);
		mainPane.setBottom(progressWithStopPane);
		final String feedFolder = datafeedLabel.getText();
		Platform.runLater(() -> {
			loadFeedzillaDataFromFileStorage(feedFolder, dateDownloadFrom);
		});
	}

	private void loadFeedzillaDataFromFileStorage(String feedFolder, LocalDateTime dateDownloadFrom) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				downloadData(feedFolder, dateDownloadFrom);
			}
		}).start();
	}

	private void downloadData(String feedFolder, LocalDateTime dateDownloadFrom) {
		try {
			final FeedzillaHashStorage hashStorage = new FeedzillaHashStorage(feedFolder);
			// hashStorage.setReceiver(new
			// ReceiverToIndicatorProcess(progressWithStopPane));
			final FeedzillaFileStorage storage = hashStorage.readFeedDataAndStore(dateDownloadFrom);
			// final Collection<>
			final Collection<FeedzillaFileArticle> articles = storage.getArticlesById();
			System.out.println("Articles size: " + articles.size());
			// final Map<LocalDateTime, List<FeedzillaFileArticle>> data =
			// storage.getArticlesByDate();
			// System.out.println(":: " + data.size());
			// int index = 0;
			// for (Entry<LocalDateTime, List<FeedzillaFileArticle>> entry :
			// data.entrySet()) {
			// System.out.println(":::: " + entry.getValue() + " = " +
			// entry.getValue().size());
			// // for (FeedzillaFileArticle article : entry.getValue()) {
			// // final int finalIndex = index;
			// // // Platform.runLater(() -> {
			// // // model.add(new FeedzillaArticleDescription(finalIndex,
			// // article.getPublishDate()));
			// // // });
			// // index += 1;
			// // }
			// }
		} catch (Exception e) {
			Dialogs.create().owner(owner).showException(e);
		}
	}

	public void setMainWindow(Stage owner) {
		this.owner = owner;
	}

	public BorderPane getMainPane() {
		return mainPane;
	}

}
